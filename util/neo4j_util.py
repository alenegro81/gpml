
def execute_without_exception(session, query):
    try:
        session.run(query)
    except Exception as e:
        pass