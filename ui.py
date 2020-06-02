import pyodbc
from tabulate import tabulate
from textwrap import fill

connection = pyodbc.connect("DSN=DBMS;UID=SA;PWD=MsSqlAni123", autocommit=True)
mycursor = connection.cursor()


def execQuery(cur, query, params=[]):
    action = query.split()[0].lower()
    if action == "create" or action == "alter":
        return "This operation is not supported"
    if action not in ("insert", "update", "delete", "select"):
        return f"\nError: {query}"
    try:
        cur.execute(query, params)
    except Exception as e:
        return f"\nThere was an error in executing the statement. Error:\n{e}"
    if action == "select":
        res = [[fill(str(i), width=35) for i in row] for row in cur.fetchall()]
        if res:
            headers = [desc[0] for desc in cur.description]
            return tabulate(res, headers, tablefmt="orgtbl")
        else:
            return "No results"
    else:
        action = action + "ed" if action == "insert" else action + "d"
        return f"{cur.rowcount} line{'s' if cur.rowcount>1 else ''} {action}"


def promptOptions(options, validops):
    op = None
    attempt = 1
    while op not in validops:
        if attempt > 1:
            print("You have entered an invalid option. Please enter a valid option.")
        print(options)
        try:
            attempt += 1
            op = int(input())
        except ValueError:
            continue
    return op


def readSql(filename):
    with open(filename) as queries:
        q = queries.readlines()
        querydict = {}
        query = {}
        section = ""
        for line in q:
            if line.strip().startswith("/*"):
                cur = line.strip()[3:-3]
                if cur.isupper():
                    if query:
                        querydict[section].append(query.copy())
                        query.clear()
                    section = cur
                    querydict[section] = []
                elif cur[0].isupper():
                    if query:
                        querydict[section].append(query.copy())
                        query.clear()
                    query["title"] = cur
                    query["params"] = []
                else:
                    query["params"].append(cur)

            else:
                query["sql"] = line.strip()
    return querydict


def headerize(title):
    return "=" * 40 + "\n" + "\t=> " + title + "\n" + "=" * 40 + "\n" * 2


def makeList(querydict):
    counter = 1
    indices = {}
    qlist = ""
    for section, queries in querydict.items():
        qlist += headerize(section)
        for i, query in enumerate(queries):
            title = query["title"]
            qlist += f"{counter}. {title}\n"
            indices[counter] = (section, i)
            counter += 1
        qlist += "\n"
    qlist += (
        headerize("Menu options")
        + f"{counter}. Return to previous menu\n{counter+1}. Exit"
    )
    return qlist, indices


def makeOpSql(operation, table, cols, cond=None):
    operation = operation.lower()
    table = table.lower()
    if operation not in ("insert", "update", "delete"):
        return "Invalid operation"
    fields = {
        "candidate": {
            "name": 1,
            "age": 1,
            "education": 0,
            "experience": 0,
            "role": 0,
            "status": 0,
        },
        "interviewer": {"employee_id": 1},
        "interview": {"candidate_id": 1, "interviewer_id": 1, "score": 0, "result": 0},
        "map": {"interview_id": 1, "question_id": 1},
    }
    if not fields.get(table, None):
        return "Table is not allowed for opration"
    if operation == "insert":
        params = []
        for col in cols:
            if col not in fields[table].keys():
                raise ValueError(f"Column '{col}' not in table")
            col_disp = col + "(*)" if fields[table][col] else col
            p = input(f"Enter value for {col_disp}: ")
            if not p:
                if fields[table][col]:
                    while not p:
                        print("This is a compulsory field!")
                        p = input(f"Enter value for {col_disp}: ")
                else:
                    p = None
            params.append(p)
        placeholder = ", ".join(["?"] * len(params))
        cols = ", ".join(cols)
        return f"insert into {table}({cols}) values ({placeholder});", params
    elif operation == "update":
        if not cond:
            raise AttributeError("Condition required for update")
        params = []
        placeholder = []
        for col in cols:
            if col not in fields[table].keys():
                raise ValueError(f"Column '{col}' not in table")
            p = input(f"Enter value for {col}(blank keeps current): ")
            if p:
                params.append(p)
                placeholder.append(f"{col}=?")
        if params:
            placeholder = ", ".join(placeholder)
            return f"update {table} set {placeholder} where {cond};", params
        else:
            return "Nothing to update!", []
    else:
        if not cond:
            raise AttributeError("Condition required for delete")
        return f"delete from {table} where {cond};"


prompt1 = """
Select option:\n
1. Query data
2. Update data
3. Exit\n
"""
prompt2 = """
Select option:\n
1. Predefined query
2. Custom query
3. Return to previous menu
4. Exit\n
"""
prompt4 = """
Select operation:\n
1. Add new candidate
2. Add new interviewer
3. Schedule interview
4. Update interview details
5. Update candidate details
6. Update candidate status
7. Return to previous menu
8. Exit\n
"""
queries = readSql("queries.sql")
prompt3, indices = makeList(queries)


def main():
    print("Welcome To InterviewBase")
    print("=" * 40)
    state = 1

    while state:
        if state == 1:
            validops = list(range(1, 4))
            op = promptOptions(prompt1, validops)
            if op == 1:
                state = 2
            elif op == 2:
                state = 5
            elif op == 3:
                state = 0
        elif state == 2:
            validops = list(range(1, 5))
            op = promptOptions(prompt2, validops)
            if op == 1:
                state = 3
            elif op == 2:
                state = 4
            elif op == 3:
                state = 1
            elif op == 4:
                state = 0
        elif state == 3:
            validops = list(range(1, len(indices) + 3))
            op = promptOptions(prompt3, validops)
            if op == len(indices) + 1:
                state = 2
                continue
            elif op == len(indices) + 2:
                state = 0
                continue
            query = queries[indices[op][0]][indices[op][1]]["sql"]
            paramPrompts = queries[indices[op][0]][indices[op][1]]["params"]
            params = []
            for prompt in paramPrompts:
                params.append(input(f"Enter {prompt}: "))
            res = execQuery(mycursor, query, params=params)
            print(res)
            input("Press ENTER to continue...")
        elif state == 4:
            sql = input("Enter query (r to return, q to quit): ")
            if sql.lower() == "r":
                state = 2
                continue
            if sql.lower() == "q":
                state = 0
                continue
            res = execQuery(mycursor, sql, [])
            print(res)
            input("Press ENTER to continue...")
        elif state == 5:
            validops = list(range(1, 9))
            op = promptOptions(prompt4, validops)
            if op == 1:
                sql, params = makeOpSql(
                    "insert",
                    "candidate",
                    ["name", "age", "education", "experience", "role"],
                )
                res = execQuery(mycursor, sql, params)
                curID = mycursor.execute("select ident_current('candidate')").fetchval()
                skills = [
                    tuple(j.strip() for j in i.strip().split(":"))
                    for i in input("Enter skills: ").split(",")
                ]
                for skill in skills:
                    execQuery(
                        mycursor,
                        "insert into skills(candidate_id, skill_name, level) values (?,?,?)",
                        [curID, skill[0], skill[1]],
                    )
                print(res)
            elif op == 2:
                sql, params = makeOpSql("insert", "interviewer", ["employee_id"])
                res = execQuery(mycursor, sql, params)
                print(res)
            elif op == 3:
                sql, params = makeOpSql(
                    "insert", "interview", ["candidate_id", "interviewer_id"]
                )
                res = execQuery(mycursor, sql, params)
                print(res)
            elif op == 4:
                interview_id = int(input("Enter interview id: "))
                sql, params = makeOpSql(
                    "update",
                    "interview",
                    ["score", "result"],
                    cond=f"interview_id={interview_id}",
                )
                res = execQuery(mycursor, sql, params)
                curID = mycursor.execute("select ident_current('interview')").fetchval()
                questions = [i for i in input("Enter questions asked: ").split(",")]
                for question in questions:
                    execQuery(
                        mycursor,
                        "insert into map(interview_id, question_id) values (?,?)",
                        [curID, question],
                    )
                print(res)
            elif op == 5:
                candidate_id = int(input("Enter candidate id: "))
                sql, params = makeOpSql(
                    "update",
                    "candidate",
                    ["name", "age", "education", "experience", "role"],
                    cond=f"candidate_id={candidate_id}",
                )
                res = execQuery(mycursor, sql, params)
                print(res)
            elif op == 6:
                candidate_id = int(input("Enter candidate id: "))
                sql, params = makeOpSql(
                    "update",
                    "candidate",
                    ["status"],
                    cond=f"candidate_id={candidate_id}",
                )
                res = execQuery(mycursor, sql, params)
                print(res)
            elif op == 7:
                state = 1
            elif op == 8:
                state = 0

    print("Thank you for using InterviewBase")


if __name__ == "__main__":
    main()
