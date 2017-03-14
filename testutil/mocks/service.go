package mocks

// MockService represents a mock Service
type MockService struct {
	Health bool
}

// IsHealthy checks the health of the service.
func (m MockService) IsHealthy() bool {
	return m.Health
}
