package domain

func ValidRoleLevel(role string) bool {
	switch role {
	case "preparator", "reviewer", "approver":
		return true
	}
	return false
}
