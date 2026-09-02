package store

import "github.com/PelicanPlatform/classad/ast"

// demoteTargetToSelf rewrites TARGET-scoped attribute references in a collector
// constraint to unscoped ones, in place.
//
// A collector query or invalidation is evaluated against a single candidate ad,
// and HTCondor's collector resolves TARGET against that candidate. A startd's
// final invalidate, for instance, sends `Requirements = TARGET.Name == "<name>"`
// (condor_startd ResMgr::final_update), and the C++ collector matches and removes
// the named ad. The vm scan has no match target during a collection scan, so a
// TARGET reference resolves to undefined and the constraint matches nothing --
// which is why forwarded invalidations were dropped (the ad was never removed).
// Unscoped and MY references already resolve against the candidate ad, so making
// TARGET behave the same matches the C++ collector's single-ad semantics.
func demoteTargetToSelf(e ast.Expr) {
	walkExprNodes(e, func(n ast.Expr) {
		if ref, ok := n.(*ast.AttributeReference); ok && ref.Scope == ast.TargetScope {
			ref.Scope = ast.NoScope
		}
	})
}

// walkExprNodes visits e and every descendant expression node.
func walkExprNodes(e ast.Expr, visit func(ast.Expr)) {
	if e == nil {
		return
	}
	visit(e)
	switch v := e.(type) {
	case *ast.BinaryOp:
		walkExprNodes(v.Left, visit)
		walkExprNodes(v.Right, visit)
	case *ast.UnaryOp:
		walkExprNodes(v.Expr, visit)
	case *ast.ParenExpr:
		walkExprNodes(v.Inner, visit)
	case *ast.ListLiteral:
		for _, el := range v.Elements {
			walkExprNodes(el, visit)
		}
	case *ast.FunctionCall:
		for _, a := range v.Args {
			walkExprNodes(a, visit)
		}
	case *ast.ConditionalExpr:
		walkExprNodes(v.Condition, visit)
		walkExprNodes(v.TrueExpr, visit)
		walkExprNodes(v.FalseExpr, visit)
	case *ast.ElvisExpr:
		walkExprNodes(v.Left, visit)
		walkExprNodes(v.Right, visit)
	case *ast.SelectExpr:
		walkExprNodes(v.Record, visit)
	case *ast.SubscriptExpr:
		walkExprNodes(v.Container, visit)
		walkExprNodes(v.Index, visit)
	case *ast.RecordLiteral:
		if v.ClassAd != nil {
			for _, a := range v.ClassAd.Attributes {
				walkExprNodes(a.Value, visit)
			}
		}
	}
}
