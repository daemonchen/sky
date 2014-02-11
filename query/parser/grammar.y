%{

package parser

import (
    "github.com/skydb/sky/db"
    "github.com/skydb/sky/query/ast"
)

%}

%union{
    token int
    integer int
    boolean bool
    str string
    strs []string
    query *ast.Query
    var_decl *ast.VarDecl
    var_decls ast.VarDecls
    statement ast.Statement
    statements ast.Statements
    assignment *ast.Assignment
    exit *ast.Exit
    debug *ast.Debug
    selection *ast.Selection
    field *ast.Field
    fields ast.Fields
    condition *ast.Condition
    condition_within *within
    event_loop *ast.EventLoop
    session_loop *ast.SessionLoop
    temporal_loop *ast.TemporalLoop
    expr ast.Expression
    var_ref *ast.VarRef
    var_refs []*ast.VarRef
    integer_literal *ast.IntegerLiteral
    boolean_literal *ast.BooleanLiteral
    string_literal *ast.StringLiteral
}

%token <token> TSTARTQUERY, TSTARTSTATEMENT, TSTARTSTATEMENTS, TSTARTEXPRESSION
%token <token> TFACTOR, TSTRING, TINTEGER, TFLOAT, TBOOLEAN
%token <token> TDECLARE, TAS, TSET, TEXIT, TDEBUG
%token <token> TSELECT, TGROUP, TBY, TINTO, TDISTINCT
%token <token> TWHEN, TWITHIN, TTHEN, TEND
%token <token> TFOR, TEACH, TEVERY, TIN, TEVENT
%token <token> TSESSION, TDELIMITED
%token <token> TSEMICOLON, TCOMMA, TLPAREN, TRPAREN, TRANGE
%token <token> TEQUALS, TNOTEQUALS, TLT, TLTE, TGT, TGTE
%token <token> TAND, TOR, TPLUS, TMINUS, TMUL, TDIV, TASSIGN
%token <token> TTRUE, TFALSE, TAMPERSAND
%token <str> TIDENT, TAMPIDENT, TQUOTEDSTRING, TWITHINUNITS, TTIMEUNITS
%token <integer> TINT

%type <query> query
%type <var_decls> var_decls
%type <var_decl> var_decl
%type <assignment> assignment
%type <exit> exit
%type <debug> debug
%type <selection> selection
%type <statement> statement
%type <statements> statements
%type <field> field
%type <fields> fields
%type <var_refs> selection_group_by, selection_dimensions
%type <str> var_name, selection_name, field_alias
%type <str> data_type, var_decl_association
%type <boolean> field_distinct

%type <condition> condition
%type <condition_within> condition_within

%type <event_loop> event_loop
%type <session_loop> session_loop
%type <temporal_loop> temporal_loop
%type <integer> temporal_loop_step temporal_loop_duration

%type <expr> expr
%type <integer_literal> integer_literal
%type <boolean_literal> boolean_literal
%type <string_literal> string_literal
%type <var_ref> var_ref

%left TASSIGN
%left TOR
%left TAND
%left TEQUALS TNOTEQUALS
%left TLT TLTE
%left TGT TGTE
%left TPLUS TMINUS
%left TMUL TDIV

%start start

%%

start :
    TSTARTQUERY query
    {
        l := yylex.(*yylexer)
        l.query = $2
    }
|   TSTARTSTATEMENTS statements
    {
        l := yylex.(*yylexer)
        l.statements = $2
    }
|   TSTARTSTATEMENT statement
    {
        l := yylex.(*yylexer)
        l.statement = $2
    }
|   TSTARTEXPRESSION expr
    {
        l := yylex.(*yylexer)
        l.expression = $2
    }
;

query :
    var_decls statements
    {
        $$ = ast.NewQuery()
        $$.DeclaredVarDecls = $1
        $$.Statements = $2
    }
;

statements :
    /* empty */
    {
        $$ = make(ast.Statements, 0)
    }
|   statements statement
    {
        $$ = append($1, $2)
    }
;

statement :
    assignment    { $$ = ast.Statement($1) }
|   exit          { $$ = ast.Statement($1) }
|   debug         { $$ = ast.Statement($1) }
|   selection     { $$ = ast.Statement($1) }
|   condition     { $$ = ast.Statement($1) }
|   event_loop    { $$ = ast.Statement($1) }
|   session_loop  { $$ = ast.Statement($1) }
|   temporal_loop { $$ = ast.Statement($1) }
;

var_decls :
    /* empty */
    {
        $$ = make(ast.VarDecls, 0)
    }
|   var_decls var_decl
    {
        $$ = append($1, $2)
    }
;

var_decl :
    TDECLARE var_name TAS data_type var_decl_association
    {
        $$ = ast.NewVarDecl(0, $2, $4)
        $$.Association = $5
    }
;

var_decl_association :
    /* empty */
    {
        $$ = ""
    }
|   TLPAREN var_name TRPAREN
    {
        $$ = $2
    }
;

data_type :
    TFACTOR  { $$ = db.Factor }
|   TSTRING  { $$ = db.String }
|   TINTEGER { $$ = db.Integer }
|   TFLOAT   { $$ = db.Float }
|   TBOOLEAN { $$ = db.Boolean }
;

assignment :
    TSET var_ref TASSIGN expr
    {
        $$ = ast.NewAssignment()
        $$.Target = $2
        $$.Expression = $4
    }
;

exit :
    TEXIT
    {
        $$ = ast.NewExit()
    }
;

debug :
    TDEBUG TLPAREN expr TRPAREN
    {
        $$ = ast.NewDebug()
        $$.Expression = $3
    }
;

selection :
    TSELECT fields selection_group_by selection_name
    {
        $$ = ast.NewSelection()
        $$.Fields = $2
        $$.Dimensions = $3
        $$.Name = $4
    }
;

fields :
    /* empty */
    {
        $$ = make(ast.Fields, 0)
    }
|   field
    {
        $$ = make(ast.Fields, 0)
        $$ = append($$, $1)
    }
|   fields TCOMMA field
    {
        $$ = append($1, $3)
    }
;

field :
    expr field_alias
    {
        $$ = ast.NewField($2, "", $1)
    }
|   TIDENT TLPAREN TRPAREN field_alias
    {
        $$ = ast.NewField($4, $1, nil)
    }
|   TIDENT TLPAREN field_distinct expr TRPAREN field_alias
    {
        $$ = ast.NewField($6, $1, $4)
        $$.Distinct = $3
    }
;

field_distinct :
    /* empty */  { $$ = false }
|   TDISTINCT   { $$ = true }
;

field_alias :
    /* empty */  { $$ = "" }
|   TAS TIDENT   { $$ = $2 }
;

selection_group_by :
    /* empty */
    {
        $$ = make([]*ast.VarRef, 0)
    }
|   TGROUP TBY selection_dimensions
    {
        $$ = $3
    }
;

selection_dimensions :
    var_ref
    {
        $$ = make([]*ast.VarRef, 0)
        $$ = append($$, $1)
    }
|   selection_dimensions TCOMMA var_ref
    {
        $$ = append($1, $3)
    }
;

selection_name :
    /* empty */
    {
        $$ = ""
    }
|   TINTO TQUOTEDSTRING
    {
        $$ = $2
    }
;

condition :
    TWHEN expr condition_within TTHEN statements TEND
    {
        $$ = ast.NewCondition()
        $$.Expression = $2
        $$.Start = $3.start
        $$.End = $3.end
        $$.UOM = $3.units
        $$.Statements = $5
    }
;

condition_within :
    /* empty */
    {
        $$ = &within{start:0, end:0, units:ast.UnitSteps}
    }
|   TWITHIN TINT TRANGE TINT TWITHINUNITS
    {
        $$ = &within{start:$2, end:$4}
        switch $5 {
        case "STEPS":
            $$.units = ast.UnitSteps
        case "SESSIONS":
            $$.units = ast.UnitSessions
        case "SECONDS":
            $$.units = ast.UnitSeconds
        }
    }
;

event_loop :
    TFOR TEACH TEVENT statements TEND
    {
        $$ = ast.NewEventLoop()
        $$.Statements = $4
    }
;

session_loop :
    TFOR TEACH TSESSION TDELIMITED TBY TINT TTIMEUNITS statements TEND
    {
        $$ = ast.NewSessionLoop()
        $$.IdleDuration = ast.TimeSpanToSeconds($6, $7)
        $$.Statements = $8
    }
;

temporal_loop :
    TFOR var_ref temporal_loop_step temporal_loop_duration statements TEND
    {
        $$ = ast.NewTemporalLoop()
        $$.Iterator = $2
        $$.Step = $3
        $$.Duration = $4
        $$.Statements = $5

        // Default steps to 1 of the unit of the duration.
        if $$.Step == 0 && $$.Duration > 0 {
            _, units := ast.SecondsToTimeSpan($4)
            $$.Step = ast.TimeSpanToSeconds(1, units)
        }
    }
;

temporal_loop_step :
    /* empty */
    {
        $$ = 0
    }
|   TEVERY TINT TTIMEUNITS
    {
        $$ = ast.TimeSpanToSeconds($2, $3)
    }
;

temporal_loop_duration :
    /* empty */
    {
        $$ = 0
    }
|   TWITHIN TINT TTIMEUNITS
    {
        $$ = ast.TimeSpanToSeconds($2, $3)
    }
;

expr :
    expr TEQUALS expr     { $$ = ast.NewBinaryExpression(ast.OpEquals, $1, $3) }
|   expr TNOTEQUALS expr  { $$ = ast.NewBinaryExpression(ast.OpNotEquals, $1, $3) }
|   expr TLT expr         { $$ = ast.NewBinaryExpression(ast.OpLessThan, $1, $3) }
|   expr TLTE expr        { $$ = ast.NewBinaryExpression(ast.OpLessThanOrEqualTo, $1, $3) }
|   expr TGT expr         { $$ = ast.NewBinaryExpression(ast.OpGreaterThan, $1, $3) }
|   expr TGTE expr        { $$ = ast.NewBinaryExpression(ast.OpGreaterThanOrEqualTo, $1, $3) }
|   expr TAND expr        { $$ = ast.NewBinaryExpression(ast.OpAnd, $1, $3) }
|   expr TOR expr         { $$ = ast.NewBinaryExpression(ast.OpOr, $1, $3) }
|   expr TPLUS expr       { $$ = ast.NewBinaryExpression(ast.OpPlus, $1, $3) }
|   expr TMINUS expr      { $$ = ast.NewBinaryExpression(ast.OpMinus, $1, $3) }
|   expr TMUL expr        { $$ = ast.NewBinaryExpression(ast.OpMultiply, $1, $3) }
|   expr TDIV expr        { $$ = ast.NewBinaryExpression(ast.OpDivide, $1, $3) }
|   var_ref               { $$ = ast.Expression($1) }
|   integer_literal       { $$ = ast.Expression($1) }
|   boolean_literal       { $$ = ast.Expression($1) }
|   string_literal        { $$ = ast.Expression($1) }
|   TLPAREN expr TRPAREN  { $$ = $2 }
;

var_ref :
    var_name
    {
        $$ = ast.NewVarRef()
        $$.Name = $1
    }
;

var_name :
    TIDENT
    {
        $$ = $1
    }
|   TAMPIDENT
    {
        $$ = $1[1:]
    }
;

integer_literal :
    TINT
    {
        $$ = ast.NewIntegerLiteral($1)
    }
;

boolean_literal :
    TTRUE
    {
        $$ = ast.NewBooleanLiteral(true)
    }
|   TFALSE
    {
        $$ = ast.NewBooleanLiteral(false)
    }
;

string_literal :
    TQUOTEDSTRING
    {
        $$ = ast.NewStringLiteral($1)
    }
;

%%

type within struct {
    start int
    end int
    units string
}
