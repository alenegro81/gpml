
def executeNoException(session, query):
    try:
        session.run(query)
    except Exception as e:
        pass