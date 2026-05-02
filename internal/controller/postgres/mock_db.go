package postgres

import (
	"context"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

// MockDB implements DBInterface for testing
type MockDB struct {
	ConnectCalled         bool
	CreateUserCalled      bool
	DropUserCalled        bool
	GrantPrivilegesCalled bool
	LastConnectionString  string
	LastUsername          string
	LastCreatedUsername   string
	LastDroppedUsername   string
	LastDropCleanupPolicy accessv1.PostgresCleanupPolicy
	CreatedUsernames      []string
	DroppedUsernames      []string
	LastPassword          string
	LastGrants            []accessv1.GrantSpec
	ConnectError          error
	CreateUserError       error
	GrantPrivilegesError  error
	Users                 []string
	Grants                map[string][]accessv1.GrantSpec
}

func NewMockDB() *MockDB {
	return &MockDB{}
}

func (m *MockDB) Connect(ctx context.Context, connectionString string) error {
	m.ConnectCalled = true
	m.LastConnectionString = connectionString
	return m.ConnectError
}

func (m *MockDB) Close(ctx context.Context) error {
	return nil
}

func (m *MockDB) CreateUser(ctx context.Context, username, password string) error {
	m.CreateUserCalled = true
	m.LastUsername = username
	m.LastCreatedUsername = username
	m.CreatedUsernames = append(m.CreatedUsernames, username)
	m.LastPassword = password
	return m.CreateUserError
}

func (m *MockDB) UpdateUserPassword(ctx context.Context, username, newPassword string) error {
	m.LastUsername = username
	m.LastPassword = newPassword
	return nil
}

func (m *MockDB) DropUser(ctx context.Context, username string, cleanupPolicy accessv1.PostgresCleanupPolicy) error {
	m.DropUserCalled = true
	m.LastDroppedUsername = username
	m.LastDropCleanupPolicy = cleanupPolicy
	m.DroppedUsernames = append(m.DroppedUsernames, username)
	return nil
}

func (m *MockDB) GetUsers(ctx context.Context) ([]string, error) {
	if m.Users != nil {
		return m.Users, nil
	}
	return []string{"user1", "user2"}, nil
}

func (m *MockDB) GrantPrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	m.GrantPrivilegesCalled = true
	m.LastGrants = grants
	return m.GrantPrivilegesError
}

func (m *MockDB) RevokePrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	return nil
}

func (m *MockDB) GetGrants(ctx context.Context) (map[string][]accessv1.GrantSpec, error) {
	if m.Grants != nil {
		return m.Grants, nil
	}
	return nil, nil
}
