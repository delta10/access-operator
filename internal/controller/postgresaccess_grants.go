package controller

import (
	"fmt"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

// diffGrants compares the current grants with the desired grants and determines which grants need to be added or revoked.
// When current is nil or empty, all desired grants will be returned in toGrant and toRevoke will be empty.
func diffGrants(current, desired []accessv1.GrantSpec) (toGrant, toRevoke []accessv1.GrantSpec) {
	currentMap := make(map[string]accessv1.GrantSpec)
	desiredMap := make(map[string]accessv1.GrantSpec)

	for _, grant := range current {
		for _, privilege := range grant.Privileges {
			normalizedGrant := normalizeGrant(grant, privilege)
			currentMap[grantKey(normalizedGrant)] = accessv1.GrantSpec{
				Database:   normalizedGrant.Database,
				Schema:     normalizedGrant.Schema,
				Privileges: normalizedGrant.Privileges,
			}
		}
	}

	for _, grant := range desired {
		for _, privilege := range grant.Privileges {
			normalizedGrant := normalizeGrant(grant, privilege)
			desiredMap[grantKey(normalizedGrant)] = accessv1.GrantSpec{
				Database:   normalizedGrant.Database,
				Schema:     normalizedGrant.Schema,
				Privileges: normalizedGrant.Privileges,
			}
		}
	}

	for key, grant := range desiredMap {
		if _, exists := currentMap[key]; !exists {
			toGrant = append(toGrant, grant)
		}
	}

	for key, grant := range currentMap {
		if _, exists := desiredMap[key]; !exists {
			toRevoke = append(toRevoke, grant)
		}
	}

	return toGrant, toRevoke
}

func normalizeGrant(grant accessv1.GrantSpec, privilege string) accessv1.GrantSpec {
	normalized := accessv1.GrantSpec{
		Database:   strings.TrimSpace(grant.Database),
		Privileges: []string{strings.ToUpper(strings.TrimSpace(privilege))},
	}

	schema := defaultSchemaName
	if grant.Schema != nil && strings.TrimSpace(*grant.Schema) != "" {
		schema = strings.TrimSpace(*grant.Schema)
	}
	normalized.Schema = &schema

	return normalized
}

func grantKey(grant accessv1.GrantSpec) string {
	schema := ""
	if grant.Schema != nil {
		schema = strings.TrimSpace(*grant.Schema)
	}

	privilege := ""
	if len(grant.Privileges) > 0 {
		privilege = strings.ToUpper(strings.TrimSpace(grant.Privileges[0]))
	}

	return fmt.Sprintf("%s:%s:%s", strings.TrimSpace(grant.Database), schema, privilege)
}
