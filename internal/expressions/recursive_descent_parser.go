package expressions

import (
	"errors"
	"strconv"
)

/*
Current grammar:
root_expression ::= root_identifier [expression_access]
expression ::= identifier [expression_access]
expression_access ::= map_access | dot_notation
map_access ::= "[" key "]" [expression]
dot_notation ::= "." identifier [expression]
root_identifier ::= identifier | "$"
key ::= IntLiteral | StringLiteral | "(" expression ")"

TODO: filtering/querying
*/

type Parser struct {
	t            *Tokenizer
	currentToken *TokenValue
	atRoot       bool
}

func InitParser(expression string, fileName string) (*Parser, error) {
	t := InitTokenizer(expression, fileName)
	p := &Parser{t: t}
	p.atRoot = true

	return p, nil
}

func (p *Parser) AdvanceToken() error {
	if p.t.HasNextToken() {
		newToken, err := p.t.GetNext()
		p.currentToken = newToken
		return err
	} else {
		p.currentToken = nil
		return nil
	}
}

func (p *Parser) ParseMapAccess(expressionToAccess ASTNode) (*MapAccessor, error) {
	if expressionToAccess == nil {
		return nil, errors.New("parameter expressionToAccess is nil")
	}
	// Verify and read in the [
	if p.currentToken == nil ||
		p.currentToken.Token_id != MapDelimiterStart {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: []TokenID{IdentifierToken}}
	}
	p.AdvanceToken()

	validTokens := []TokenID{StringLiteral, IntLiteral, ExpressionStart}

	// Read in the key
	if p.currentToken == nil || !sliceContains(validTokens, p.currentToken.Token_id) {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: validTokens}
	}
	var key *Key
	// Bracket access notation allows string literals, int literals, and sub-expressions
	if p.currentToken.Token_id == StringLiteral {
		// The literal token includes the "", so trim the ends off.
		key = &Key{Literal: &ASTStringLiteral{StrValue: p.currentToken.Value[1 : len(p.currentToken.Value)-1]}}
	} else if p.currentToken.Token_id == IntLiteral {
		parsedInt, err := strconv.Atoi(p.currentToken.Value)
		if err != nil {
			return nil, err // Should not fail if the parser is setup correctly
		}
		key = &Key{Literal: &ASTIntLiteral{IntValue: parsedInt}}
	} else if p.currentToken.Token_id == ExpressionStart {
		p.AdvanceToken() // Read past (
		node, err := p.ParseSubExpression()
		if err != nil {
			return nil, err
		}
		key = &Key{SubExpression: node}

		// Verify that next token is end of expression )
		if p.currentToken == nil || p.currentToken.Token_id != ExpressionEnd {
			return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: []TokenID{ExpressionEnd}}
		}
	}
	p.AdvanceToken()

	// Verify and read in the ]
	if p.currentToken == nil ||
		p.currentToken.Token_id != MapDelimiterEnd {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: []TokenID{IdentifierToken}}
	}
	p.AdvanceToken()

	return &MapAccessor{LeftNode: expressionToAccess, RightKey: key}, nil

}

func (p *Parser) ParseIdentifier() (*Identifier, error) {
	// Only accessing one token, the identifier
	if p.currentToken == nil ||
		p.currentToken.Token_id != IdentifierToken {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: []TokenID{IdentifierToken}}
	}

	parsedIdentifier := &Identifier{IdentifierName: p.currentToken.Value}
	p.AdvanceToken()
	return parsedIdentifier, nil
}

func (p *Parser) ParseExpression() (ASTNode, error) {
	err := p.AdvanceToken()
	if err != nil {
		return nil, err
	}

	node, err := p.ParseSubExpression()
	if p.currentToken != nil {
		// Reached wrong token. It should be at the end here.
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: nil}
	}
	return node, err
}

// This function parses all of the dot notations and map accesses
func (p *Parser) ParseSubExpression() (ASTNode, error) {
	supportedTokens := []TokenID{RootAccess, CurrentNodeAccess, IdentifierToken}
	// The first identifier should always be the root identifier, $
	if p.currentToken == nil || !sliceContains(supportedTokens, p.currentToken.Token_id) {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: supportedTokens}
	} else if p.atRoot && p.currentToken.Token_id == CurrentNodeAccess {
		// Can't support @ at root
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedTokens: []TokenID{RootAccess, IdentifierToken}}
	}
	if p.atRoot {
		p.atRoot = false // No longer allow $
	}

	var parsed ASTNode = &Identifier{IdentifierName: p.currentToken.Value}
	p.AdvanceToken()

	for {

		if p.currentToken == nil {
			// Reached end
			return parsed, nil
		} else if p.currentToken.Token_id == DotObjectAccess {
			// Dot notation
			p.AdvanceToken() // Move past the .
			accessingIdentifier, err := p.ParseIdentifier()
			if err != nil {
				return nil, err
			}
			parsed = &DotNotation{LeftAccessableNode: parsed, RightAccessIdentifier: accessingIdentifier}
		} else if p.currentToken.Token_id == MapDelimiterStart {
			// Bracket notation
			parsedMapAccess, err := p.ParseMapAccess(parsed)
			if err != nil {
				return nil, err
			}
			parsed = parsedMapAccess
		} else {
			// Reached a token this function is not responsible for
			return parsed, nil
		}
	}
}

func sliceContains(slice []TokenID, value TokenID) bool {
	for _, val := range slice {
		if val == value {
			return true
		}
	}
	return false
}
