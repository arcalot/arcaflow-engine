package expressions

// WalkAST walks the AST in-order.
func WalkAST(
	ast ASTNode,
	beforeNode func(node ASTNode) error,
	onNode func(node ASTNode) error,
	afterNode func(node ASTNode) error,
) error {
	if err := beforeNode(ast); err != nil {
		return err
	}
	if left := ast.Left(); left != nil {
		if err := WalkAST(left, beforeNode, onNode, afterNode); err != nil {
			return err
		}
	}
	if err := onNode(ast); err != nil {
		return err
	}
	if right := ast.Right(); right != nil {
		if err := WalkAST(right, beforeNode, onNode, afterNode); err != nil {
			return err
		}
	}
	if err := afterNode(ast); err != nil {
		return err
	}
	return nil
}
