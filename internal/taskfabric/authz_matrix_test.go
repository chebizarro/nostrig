package taskfabric

import "testing"

func TestAuthorizationRoleMethodMatrixEveryCell(t *testing.T) {
	methods := []string{
		"task/create",
		"task/claim",
		"task/assign",
		"task/update",
		"task/close",
		"task/delete",
		"task/quality-status",
		"queue/enqueue",
		"queue/dequeue",
		"queue/list",
	}
	roles := []Role{
		RoleAdmin,
		RoleOperator,
		RoleMaintainer,
		RoleDispatcher,
		RoleWorker,
		RoleReviewer,
		RoleReadOnly,
	}
	allowed := map[Role]map[string]bool{
		RoleAdmin:      allowMethods(methods...),
		RoleOperator:   allowMethods(methods...),
		RoleMaintainer: allowMethods(methods...),
		RoleDispatcher: allowMethods(methods...),
		RoleWorker: allowMethods(
			"task/claim", "task/update", "task/close", "task/quality-status",
			"queue/dequeue", "queue/list",
		),
		RoleReviewer: allowMethods("task/quality-status", "task/close", "queue/list"),
		RoleReadOnly: allowMethods("task/quality-status", "queue/list"),
	}

	for _, method := range methods {
		if !supportedMethod(method) {
			t.Fatalf("test matrix contains unsupported method %q", method)
		}
	}
	for _, role := range roles {
		if !validRole(role) {
			t.Fatalf("test matrix contains invalid role %q", role)
		}
		for _, method := range methods {
			t.Run(string(role)+"/"+method, func(t *testing.T) {
				if got, want := roleAllows(role, method), allowed[role][method]; got != want {
					t.Fatalf("roleAllows(%q, %q)=%v, want %v", role, method, got, want)
				}
			})
		}
	}

	if roleAllows(Role("unknown"), "queue/list") {
		t.Fatal("unknown role was allowed")
	}
	if supportedMethod("unknown/method") {
		t.Fatal("unknown method was reported as supported")
	}
}

func allowMethods(methods ...string) map[string]bool {
	out := make(map[string]bool, len(methods))
	for _, method := range methods {
		out[method] = true
	}
	return out
}
