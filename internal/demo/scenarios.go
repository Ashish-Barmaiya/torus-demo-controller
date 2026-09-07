package demo

import (
	"fmt"
	"net/http"
)

type Service string

const (
	ServiceUsers  Service = "users"
	ServiceOrders Service = "orders"
)

func (s Service) Valid() bool {
	switch s {
	case ServiceUsers, ServiceOrders:
		return true
	default:
		return false
	}
}

type Operation string

const (
	OperationGetUsers   Operation = "get_users"
	OperationGetUser    Operation = "get_user"
	OperationCreateUser Operation = "create_user"
	OperationUpdateUser Operation = "update_user"
	OperationDeleteUser Operation = "delete_user"

	OperationGetOrders   Operation = "get_orders"
	OperationGetOrder    Operation = "get_order"
	OperationCreateOrder Operation = "create_order"
	OperationUpdateOrder Operation = "update_order"
	OperationDeleteOrder Operation = "delete_order"
)

func (o Operation) Valid() bool {
	switch o {
	case OperationGetUsers,
		OperationGetUser,
		OperationCreateUser,
		OperationUpdateUser,
		OperationDeleteUser,
		OperationGetOrders,
		OperationGetOrder,
		OperationCreateOrder,
		OperationUpdateOrder,
		OperationDeleteOrder:
		return true
	default:
		return false
	}
}

func (o Operation) Service() Service {
	switch o {
	case OperationGetUsers,
		OperationGetUser,
		OperationCreateUser,
		OperationUpdateUser,
		OperationDeleteUser:
		return ServiceUsers

	case OperationGetOrders,
		OperationGetOrder,
		OperationCreateOrder,
		OperationUpdateOrder,
		OperationDeleteOrder:
		return ServiceOrders

	default:
		return ""
	}
}

func (o Operation) Method() string {
	switch o {
	case OperationGetUsers, OperationGetUser,
		OperationGetOrders, OperationGetOrder:
		return http.MethodGet

	case OperationCreateUser, OperationCreateOrder:
		return http.MethodPost

	case OperationUpdateUser, OperationUpdateOrder:
		return http.MethodPatch

	case OperationDeleteUser, OperationDeleteOrder:
		return http.MethodDelete

	default:
		return ""
	}
}

type Simulation string

const (
	SimulationNormal Simulation = "normal"
	SimulationSlow   Simulation = "slow"
	SimulationError  Simulation = "error"
)

func (s Simulation) Valid() bool {
	switch s {
	case SimulationNormal, SimulationSlow, SimulationError:
		return true
	default:
		return false
	}
}

type RequestSize string

const (
	RequestSizeNone  RequestSize = "0b"
	RequestSize1KB   RequestSize = "1kb"
	RequestSize16KB  RequestSize = "16kb"
	RequestSize64KB  RequestSize = "64kb"
	RequestSize256KB RequestSize = "256kb"
	RequestSize1MB   RequestSize = "1mb"
	RequestSize4MB   RequestSize = "4mb"
)

func (s RequestSize) Valid() bool {
	switch s {
	case RequestSizeNone,
		RequestSize1KB,
		RequestSize16KB,
		RequestSize64KB,
		RequestSize256KB,
		RequestSize1MB,
		RequestSize4MB:
		return true
	default:
		return false
	}
}

type ResponseSize string

const (
	ResponseSizeNone  ResponseSize = "0b"
	ResponseSize1KB   ResponseSize = "1kb"
	ResponseSize16KB  ResponseSize = "16kb"
	ResponseSize64KB  ResponseSize = "64kb"
	ResponseSize256KB ResponseSize = "256kb"
	ResponseSize1MB   ResponseSize = "1mb"
	ResponseSize4MB   ResponseSize = "4mb"
)

func (s ResponseSize) Valid() bool {
	switch s {
	case ResponseSizeNone,
		ResponseSize1KB,
		ResponseSize16KB,
		ResponseSize64KB,
		ResponseSize256KB,
		ResponseSize1MB,
		ResponseSize4MB:
		return true
	default:
		return false
	}
}

type Scenario struct {
	Service      Service      `json:"service"`
	Operation    Operation    `json:"operation"`
	Simulation   Simulation   `json:"simulation"`
	RequestSize  RequestSize  `json:"request_size"`
	ResponseSize ResponseSize `json:"response_size"`
	RequestCount int          `json:"request_count"`
}

func (s Scenario) Validate() error {
	if !s.Service.Valid() {
		return fmt.Errorf("unsupported service %q", s.Service)
	}

	if !s.Operation.Valid() {
		return fmt.Errorf("unsupported operation %q", s.Operation)
	}

	if s.Operation.Service() != s.Service {
		return fmt.Errorf(
			"operation %q does not belong to service %q",
			s.Operation,
			s.Service,
		)
	}

	if !s.Simulation.Valid() {
		return fmt.Errorf("unsupported simulation %q", s.Simulation)
	}

	if !s.RequestSize.Valid() {
		return fmt.Errorf(
			"unsupported request size %q",
			s.RequestSize,
		)
	}

	if !s.ResponseSize.Valid() {
		return fmt.Errorf(
			"unsupported response size %q",
			s.ResponseSize,
		)
	}

	if s.RequestCount < 1 || s.RequestCount > 50 {
		return fmt.Errorf(
			"request count must be between 1 and 50",
		)
	}

	return nil
}
