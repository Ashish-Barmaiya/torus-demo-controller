package execution

import "github.com/Ashish-Barmaiya/torus-demo-controller/internal/request"

type testIDSource struct {
	userID  string
	orderID string
}

func (s testIDSource) User() request.UserRef {
	return request.UserRef{
		PathID: "5",
		ID:     s.userID,
	}
}

func (s testIDSource) Order() request.OrderRef {
	return request.OrderRef{
		PathID: "7",
		ID:     s.orderID,
	}
}
