/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/jackc/pgx/v5"
)

// DBInterface defines the database operations needed by the controller
type DBInterface interface {
	Connect(ctx context.Context, connectionString string) error
	Close(ctx context.Context) error
	CreateUser(ctx context.Context, username, password string) error
	UpdateUserPassword(ctx context.Context, username, newPassword string) error
	DropUser(ctx context.Context, username string, cleanupPolicy accessv1.PostgresCleanupPolicy) error
	GetUsers(ctx context.Context) ([]string, error)
	GrantPrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error
	RevokePrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error
	GetGrants(ctx context.Context) (map[string][]accessv1.GrantSpec, error)
}

// DB implements DBInterface using pgx
type DB struct {
	conn *pgx.Conn
}

const defaultSchemaName = "public"

func NewPostgresDB() *DB {
	return &DB{}
}

func (p *DB) Connect(ctx context.Context, connectionString string) error {
	conn, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		return err
	}
	p.conn = conn
	return nil
}

func (p *DB) Close(ctx context.Context) error {
	if p.conn != nil {
		return p.conn.Close(ctx)
	}
	return nil
}

func (p *DB) CreateUser(ctx context.Context, username, password string) error {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	var exists bool
	if err := p.conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", username).Scan(&exists); err != nil {
		return err
	}

	sanitizedUser := pgx.Identifier{username}.Sanitize()
	sanitizedPass := pgx.Identifier{password}.Sanitize()

	var err error
	if exists {
		_, err = p.conn.Exec(ctx, fmt.Sprintf("ALTER ROLE %s WITH LOGIN PASSWORD '%s'", sanitizedUser, sanitizedPass))
		return err
	}

	_, err = p.conn.Exec(ctx, fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s'", sanitizedUser, sanitizedPass))
	return err
}

func (p *DB) UpdateUserPassword(ctx context.Context, username, newPassword string) error {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	sanitizedUser := pgx.Identifier{username}.Sanitize()
	sanitizedPass := pgx.Identifier{newPassword}.Sanitize()

	_, err := p.conn.Exec(ctx, fmt.Sprintf("ALTER ROLE %s WITH LOGIN PASSWORD '%s'", sanitizedUser, sanitizedPass))
	return err
}

func (p *DB) DropUser(ctx context.Context, username string, policy accessv1.PostgresCleanupPolicy) (err error) {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}
	if username == "" {
		return fmt.Errorf("username is empty")
	}

	// Idempotency: DROP OWNED/REASSIGN OWNED require the role to exist
	var exists bool
	if err := p.conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, username,
	).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check role existence: %w", err)
	}
	if !exists {
		return nil
	}

	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	u := pgx.Identifier{username}.Sanitize()

	switch policy {
	case accessv1.CleanupPolicyCascade:
		if _, err = tx.Exec(ctx, fmt.Sprintf(`DROP OWNED BY %s CASCADE`, u)); err != nil {
			return fmt.Errorf("drop owned cascade: %w", err)
		}

	case accessv1.CleanupPolicyOrphan:
		var target string
		if err := tx.QueryRow(ctx, `
			SELECT pg_catalog.pg_get_userbyid(d.datdba)
			FROM pg_database d
			WHERE d.datname = current_database()`,
		).Scan(&target); err != nil {
			return fmt.Errorf("get current database owner: %w", err)
		}
		t := pgx.Identifier{target}.Sanitize()

		if _, err = tx.Exec(ctx, fmt.Sprintf(`REASSIGN OWNED BY %s TO %s`, u, t)); err != nil {
			return fmt.Errorf("reassign owned: %w", err)
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf(`DROP OWNED BY %s`, u)); err != nil {
			return fmt.Errorf("drop owned (privileges): %w", err)
		}

	case accessv1.CleanupPolicyRestrict:
		// Comprehensive ownership check: cluster-wide shared objects AND database-local objects
		var ownsAnything bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				-- Cluster-wide shared objects (databases, tablespaces, etc.)
				SELECT 1
				FROM pg_shdepend d
				JOIN pg_roles r ON r.oid = d.refobjid
				WHERE r.rolname = $1
				  AND d.deptype = 'o'
				UNION ALL
				-- Database-local objects (tables, views, sequences, etc.)
				SELECT 1
				FROM pg_class c
				JOIN pg_roles r ON r.oid = c.relowner
				WHERE r.rolname = $1
				UNION ALL
				-- Functions and procedures
				SELECT 1
				FROM pg_proc p
				JOIN pg_roles r ON r.oid = p.proowner
				WHERE r.rolname = $1
				UNION ALL
				-- Types
				SELECT 1
				FROM pg_type t
				JOIN pg_roles r ON r.oid = t.typowner
				WHERE r.rolname = $1
				UNION ALL
				-- Schemas
				SELECT 1
				FROM pg_namespace n
				JOIN pg_roles r ON r.oid = n.nspowner
				WHERE r.rolname = $1
			)`, username,
		).Scan(&ownsAnything); err != nil {
			return fmt.Errorf("check ownership: %w", err)
		}
		if ownsAnything {
			return fmt.Errorf("cannot drop user %q: user owns objects (cleanupPolicy Restrict)", username)
		}

		// Safe now: only revokes privileges (no owned objects to drop)
		if _, err = tx.Exec(ctx, fmt.Sprintf(`DROP OWNED BY %s`, u)); err != nil {
			return fmt.Errorf("drop owned (privileges): %w", err)
		}

	case accessv1.CleanupPolicyNone:
		return fmt.Errorf("cannot drop user %q with cleanup policy None", username)

	default:
		return fmt.Errorf("unknown cleanup policy: %s", policy)
	}

	if _, err = tx.Exec(ctx, fmt.Sprintf(`DROP ROLE %s`, u)); err != nil {
		return fmt.Errorf("drop role %q: %w", username, err)
	}
	return nil
}

func (p *DB) GetUsers(ctx context.Context) ([]string, error) {
	if p.conn == nil {
		return nil, fmt.Errorf("database connection is not initialized")
	}

	rows, err := p.conn.Query(ctx, "SELECT rolname FROM pg_roles WHERE rolcanlogin = true and rolsuper = false")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, err
		}
		users = append(users, username)
	}
	return users, nil
}

func (p *DB) GrantPrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	quotedUsername := pgx.Identifier{username}.Sanitize()
	for _, grant := range grants {
		schema := defaultSchemaName
		if grant.Schema != nil && *grant.Schema != "" {
			schema = *grant.Schema
		}

		databasePrivileges := make([]string, 0, len(grant.Privileges))
		schemaPrivileges := make([]string, 0, len(grant.Privileges))
		tablePrivileges := make([]string, 0, len(grant.Privileges))
		functionPrivileges := make([]string, 0, len(grant.Privileges))
		unsupportedPrivileges := make([]string, 0)

		for _, privilege := range grant.Privileges {
			switch strings.ToUpper(privilege) {
			case "CONNECT", "TEMPORARY":
				databasePrivileges = append(databasePrivileges, privilege)
			case "CREATE":
				// CREATE can apply to both DATABASE (create schema) and SCHEMA (create objects).
				databasePrivileges = append(databasePrivileges, privilege)
				schemaPrivileges = append(schemaPrivileges, privilege)
			case "USAGE":
				schemaPrivileges = append(schemaPrivileges, privilege)
			case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN":
				tablePrivileges = append(tablePrivileges, privilege)
			case "EXECUTE":
				functionPrivileges = append(functionPrivileges, privilege)
			default:
				unsupportedPrivileges = append(unsupportedPrivileges, privilege)
			}
		}

		if len(unsupportedPrivileges) > 0 {
			return fmt.Errorf("unsupported privileges: %s", strings.Join(unsupportedPrivileges, ", "))
		}

		quotedDatabase := pgx.Identifier{grant.Database}.Sanitize()
		quotedSchema := pgx.Identifier{schema}.Sanitize()

		if len(databasePrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON DATABASE %s TO %s",
					strings.Join(databasePrivileges, ", "),
					quotedDatabase,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(schemaPrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON SCHEMA %s TO %s",
					strings.Join(schemaPrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(tablePrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO %s",
					strings.Join(tablePrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(functionPrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON ALL FUNCTIONS IN SCHEMA %s TO %s",
					strings.Join(functionPrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *DB) RevokePrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	quotedUsername := pgx.Identifier{username}.Sanitize()
	for _, grant := range grants {
		schema := defaultSchemaName
		if grant.Schema != nil && *grant.Schema != "" {
			schema = *grant.Schema
		}

		databasePrivileges := make([]string, 0, len(grant.Privileges))
		schemaPrivileges := make([]string, 0, len(grant.Privileges))
		tablePrivileges := make([]string, 0, len(grant.Privileges))
		functionPrivileges := make([]string, 0, len(grant.Privileges))
		unsupportedPrivileges := make([]string, 0)

		for _, privilege := range grant.Privileges {
			switch strings.ToUpper(privilege) {
			case "CONNECT", "TEMPORARY":
				databasePrivileges = append(databasePrivileges, privilege)
			case "CREATE":
				databasePrivileges = append(databasePrivileges, privilege)
				schemaPrivileges = append(schemaPrivileges, privilege)
			case "USAGE":
				schemaPrivileges = append(schemaPrivileges, privilege)
			case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN":
				tablePrivileges = append(tablePrivileges, privilege)
			case "EXECUTE":
				functionPrivileges = append(functionPrivileges, privilege)
			default:
				unsupportedPrivileges = append(unsupportedPrivileges, privilege)
			}
		}

		if len(unsupportedPrivileges) > 0 {
			return fmt.Errorf("unsupported privileges: %s", strings.Join(unsupportedPrivileges, ", "))
		}

		quotedDatabase := pgx.Identifier{grant.Database}.Sanitize()
		quotedSchema := pgx.Identifier{schema}.Sanitize()

		if len(databasePrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("REVOKE %s ON DATABASE %s FROM %s",
					strings.Join(databasePrivileges, ", "),
					quotedDatabase,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(schemaPrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("REVOKE %s ON SCHEMA %s FROM %s",
					strings.Join(schemaPrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(tablePrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("REVOKE %s ON ALL TABLES IN SCHEMA %s FROM %s",
					strings.Join(tablePrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(functionPrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("REVOKE %s ON ALL FUNCTIONS IN SCHEMA %s FROM %s",
					strings.Join(functionPrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *DB) GetGrants(ctx context.Context) (map[string][]accessv1.GrantSpec, error) {
	if p.conn == nil {
		return nil, fmt.Errorf("database connection is not initialized")
	}

	rows, err := p.conn.Query(ctx, `
		WITH database_grants AS (
			SELECT
				r.rolname AS grantee,
				d.datname AS database_name,
				NULL::text AS schema_name,
				a.privilege_type
			FROM pg_database d
			JOIN LATERAL aclexplode(d.datacl) AS a ON true
			JOIN pg_roles r ON r.oid = a.grantee
			WHERE d.datname = current_database()
		),
		schema_grants AS (
			SELECT
				r.rolname AS grantee,
				current_database() AS database_name,
				n.nspname AS schema_name,
				a.privilege_type
			FROM pg_namespace n
			JOIN LATERAL aclexplode(n.nspacl) AS a ON true
			JOIN pg_roles r ON r.oid = a.grantee
			WHERE n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
			  AND n.nspname <> 'information_schema'
		),
		table_totals AS (
			SELECT
				n.nspname AS schema_name,
				count(*) AS total_tables
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
			  AND n.nspname <> 'information_schema'
			GROUP BY n.nspname
		),
		table_grants AS (
			SELECT
				r.rolname AS grantee,
				current_database() AS database_name,
				n.nspname AS schema_name,
				a.privilege_type,
				count(DISTINCT c.oid) AS granted_tables
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			JOIN LATERAL aclexplode(c.relacl) AS a ON true
			JOIN pg_roles r ON r.oid = a.grantee
			WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
			  AND n.nspname <> 'information_schema'
			GROUP BY r.rolname, n.nspname, a.privilege_type
		),
		function_totals AS (
			SELECT
				n.nspname AS schema_name,
				count(*) AS total_functions
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
			  AND n.nspname <> 'information_schema'
			GROUP BY n.nspname
		),
		function_grants AS (
			SELECT
				r.rolname AS grantee,
				current_database() AS database_name,
				n.nspname AS schema_name,
				a.privilege_type,
				count(DISTINCT p.oid) AS granted_functions
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			JOIN LATERAL aclexplode(p.proacl) AS a ON true
			JOIN pg_roles r ON r.oid = a.grantee
			WHERE n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
			  AND n.nspname <> 'information_schema'
			GROUP BY r.rolname, n.nspname, a.privilege_type
		)
		SELECT grantee, database_name, schema_name, privilege_type
		FROM database_grants
		UNION ALL
		SELECT grantee, database_name, schema_name, privilege_type
		FROM schema_grants
		UNION ALL
		SELECT tg.grantee, tg.database_name, tg.schema_name, tg.privilege_type
		FROM table_grants tg
		JOIN table_totals tt ON tt.schema_name = tg.schema_name
		WHERE tg.granted_tables = tt.total_tables
		UNION ALL
		SELECT fg.grantee, fg.database_name, fg.schema_name, fg.privilege_type
		FROM function_grants fg
		JOIN function_totals ft ON ft.schema_name = fg.schema_name
		WHERE fg.granted_functions = ft.total_functions
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	returnValue := make(map[string][]accessv1.GrantSpec)
	grantIndex := make(map[string]int)

	for rows.Next() {
		var grantee string
		var database string
		var schema sql.NullString
		var privilegeType string
		if err := rows.Scan(&grantee, &database, &schema, &privilegeType); err != nil {
			return nil, err
		}

		schemaName := ""
		if schema.Valid {
			schemaName = schema.String
		}

		key := fmt.Sprintf("%s:%s:%s", grantee, database, schemaName)
		if idx, ok := grantIndex[key]; ok {
			returnValue[grantee][idx].Privileges = append(returnValue[grantee][idx].Privileges, strings.ToUpper(privilegeType))
			continue
		}

		var schemaRef *string
		if schema.Valid && schema.String != "" {
			s := schema.String
			schemaRef = &s
		}

		returnValue[grantee] = append(returnValue[grantee], accessv1.GrantSpec{
			Database:   database,
			Schema:     schemaRef,
			Privileges: []string{strings.ToUpper(privilegeType)},
		})
		grantIndex[key] = len(returnValue[grantee]) - 1
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return returnValue, nil
}
