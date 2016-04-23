import sqlparse


sql = """
with my_shit as (
    select 1
)
select rank() over (partition by index),
some_field from

my_function(events, 'created_at', 'last_signup')
join a_table
order by 1
group by 2
limit 3
"""

statement = sqlparse.parse(sql)[0]

def debug(token):
    params = [p for p in token.get_parameters()]
    #print token.get_real_name(), [p.value for p in params]

    return {
        'name': 'MY_NEW_CTE',
        'definition': "SELECT %s FROM WTF" % (", ".join(p.value for p in params))
    }

functions = {
    'my_function': debug
}

def generate_token(cte):
    sbq = "\n{} as ( {} )\n".format(cte['name'], cte['definition'])
    new_token = sqlparse.sql.Token(None, sbq)
    return new_token

def add_ctes(stmt, new_ctes):
    token = stmt.token_first()
    while token is not None:
        token_index = stmt.token_index(token)

        if token.ttype == sqlparse.tokens.Token.Keyword and token.value.lower() == 'with':
            for cte in new_ctes:
                stmt.insert_after(token, generate_token(cte))

        token = stmt.token_next(token_index)

def extract_deps(stmt):
    token = stmt.token_first()

    new_ctes = []

    while token is not None:
        token_index = stmt.token_index(token)
        if token.is_group():
            if type(token) == sqlparse.sql.Function:
                function_name = token.get_real_name()
                if function_name in functions:
                    cte = functions[function_name](token)
                    new_token = sqlparse.sql.Token('Identifier', cte['name'])
                    new_ctes.append(cte)
                    stmt.tokens[token_index] = new_token
            else:
                extract_deps(token)

        token = stmt.token_next(token_index)

    return new_ctes

new_ctes = extract_deps(statement)

add_ctes(statement, new_ctes)

print statement
