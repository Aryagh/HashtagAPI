from neo4j import GraphDatabase, basic_auth

class NeoConnector:
    def __init__(self):
        db_port = "bolt://localhost:7687"
        db_auth = ("neo4j", "admin")
        self.driver = GraphDatabase.driver(db_port, auth=basic_auth(db_auth[0], db_auth[1]))

###########################################################################

    def create_user_node(self, user_info):
        query = """ merge (u:I_USER  {uid:{uid_}}) set u+={user_info} """
        with self.driver.session() as session:
            session.run(query, uid_=user_info['user_id'], user_info=user_info)

    def create_user_relationship(self, user1, user2):
        query = """
        match(u1 {uid: {user1}})
        match(u2 {uid: {user2}})
        merge(u2)-[r:Connect]->(u1)"""
        with self.driver.session() as session:
            session.run(query, user1=user1, user2=user2).data()

###########################################################################

    def create_hashtag_node(self, hashtag):
        query = """ merge (h:I_HASHTAG {name:{name}})"""
        with self.driver.session() as session:
            session.run(query, name=hashtag)

    def create_hashtag_relationship(self, name1, name2):
        query = """
        match(u1 {name: {name1}})
        match(u2 {name: {name2}})
        merge(u2)-[r:Connect]->(u1)"""
        with self.driver.session() as session:
            session.run(query, name1=name1, name2=name2).data()

###########################################################################

    def get_user_info(self, uid):
        query = """match(u:I_USER{uid:{uid_}}) return u{.*} as user_info"""
        try:
            with self.driver.session() as session:
                result = session.run(query, uid_=uid).data()
                output = result[0]['user_info']['Hashtags']
                print(output)
        except:
            print('Not valid user id')

###########################################################################

if __name__ == "__main__":
    neo = NeoConnector()
    neo.get_user_info(uid="5c9ac75766204a44cc47fb11")