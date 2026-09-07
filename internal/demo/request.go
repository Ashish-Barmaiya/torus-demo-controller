package demo

type RunScenarioRequest struct {
	Service      Service      `json:"service"`
	Operation    Operation    `json:"operation"`
	Simulation   Simulation   `json:"simulation"`
	RequestSize  RequestSize  `json:"request_size"`
	ResponseSize ResponseSize `json:"response_size"`
	RequestCount int          `json:"request_count"`
}

func (r RunScenarioRequest) Scenario() Scenario {
	requestCount := r.RequestCount

	if requestCount == 0 {
		requestCount = 1
	}

	return Scenario{
		Service:      r.Service,
		Operation:    r.Operation,
		Simulation:   r.Simulation,
		RequestSize:  r.RequestSize,
		ResponseSize: r.ResponseSize,
		RequestCount: requestCount,
	}
}
