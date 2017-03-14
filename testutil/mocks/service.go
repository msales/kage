package mocks

type MockService struct {
	Health bool
}

func (m MockService) IsHealthy() bool {
	return m.Health
}
