package expressions

/*
Current grammar:
root: dotnotation
dotnotation: complex-identifier ObjectAccerss complex-identifier | dotnotation ObjectAccess complex-identifier
complex-identifier: IdentifierToken | IdentifierToken map-accessor | RootAccess
map-accessor: MapDelimiterStart key MapDelimiterEnd
key: literal | sub-expression
literal: StringLiteral | IntLiteral
sub-expression: ExpressionStart dotnotation ExpressionEnd

TODO: filtering/querying
*/

type Parser struct {
	t            *Tokenizer
	currentToken *TokenValue
}

func InitParser(expression string, fileName string) (*Parser, error) {
	t := InitTokenizer(expression, fileName)
	p := &Parser{t: t}
	err := p.AdvanceToken()
	if err != nil {
		return nil, err
	}

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

func (p *Parser) ParseMapAccess(identifier *Identifier) (*MapAccessor, error) {
	// Verify and read in the [
	if p.currentToken == nil ||
		p.currentToken.Token_id != MapDelimiterStart {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedToken: IdentifierToken}
	}
	p.AdvanceToken()

	// Read in the key
	p.AdvanceToken()

	// Verify and read in the ]
	if p.currentToken == nil ||
		p.currentToken.Token_id != MapDelimiterEnd {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedToken: IdentifierToken}
	}
	p.AdvanceToken()

	return &MapAccessor{LeftNode: identifier, RightKey: &Key{}}, nil

}

func (p *Parser) ParseIdentifier() (*Identifier, error) {
	// Only accessing one token, the identifier
	if p.currentToken == nil ||
		p.currentToken.Token_id != IdentifierToken {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedToken: IdentifierToken}
	}

	parsedIdentifier := &Identifier{IdentifierName: p.currentToken.Value}
	p.AdvanceToken()
	return parsedIdentifier, nil
}

// Parses identifiers, which may have a map access after
func (p *Parser) ParseComplexIdentifier() (ASTNode, error) {
	// Always starts with an identifier
	result, err := p.ParseIdentifier()
	if err != nil {
		return result, err
	}
	// checks to see if there is map access
	if p.currentToken != nil && p.currentToken.Token_id == MapDelimiterStart {
		return p.ParseMapAccess(result)
	} else {
		return result, nil
	}
}

// This function parses all of the dot notations
func (p *Parser) ParseRoot() (ASTNode, error) {
	// The first identifier should always be the root identifier, $
	if p.currentToken.Token_id != RootAccess {
		return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedToken: IdentifierToken}
	}

	var parsed ASTNode = &Identifier{IdentifierName: p.currentToken.Value}
	p.AdvanceToken()

	for {

		if p.currentToken != nil && p.currentToken.Token_id == DotObjectAccess {
			p.AdvanceToken() // Move past the .
			accessingIdentifier, err := p.ParseComplexIdentifier()
			if err != nil {
				return nil, err
			}
			parsed = &DotNotation{LeftAccessableNode: parsed, RightAccessIdentifier: accessingIdentifier}
		} else if p.currentToken == nil {
			return parsed, nil
		} else {
			// Reached wrong token
			return nil, &InvalidGrammarError{FoundToken: p.currentToken, ExpectedToken: "end"}
		}
	}
}
