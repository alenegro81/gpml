def create_constraints(self):
    with self._driver.session() as session:
        session.run("CREATE CONSTRAINT ON (u:Tag) ASSERT (u.id) IS NODE KEY")
        session.run("CREATE CONSTRAINT ON (i:TagOccurrence) ASSERT (i.id) IS NODE KEY")
        session.run("CREATE CONSTRAINT ON (t:Sentence) ASSERT (t.id) IS NODE KEY")
        session.run("CREATE CONSTRAINT ON (l:AnnotatedText) ASSERT (l.id) IS NODE KEY")
        session.run("CREATE CONSTRAINT ON (l:NamedEntity) ASSERT (l.id) IS NODE KEY")
