import sys, getopt

help_message = '-u <neo4j username> -p <password> -s <source directory> -b <bolt uri>'


def get_main_parameters(command, argv, extended_options='', extented_long_options=[]) -> object:
    neo4j_user = 'neo4j'
    neo4j_password = 'password'
    source_dataset_path = ''
    uri = 'bolt://localhost:7687'

    try:
        opts, args = getopt.getopt(argv, 'hu:p:s:b:'+extended_options, ['help', 'neo4j-user=', 'neo4j-password=', 'source-path=', 'bolt=']+extented_long_options)
    except getopt.GetoptError as e:
        print(e)
        print(command , help_message)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(command , help_message)
            sys.exit()
        elif opt in ("-u", "--neo4j-user"):
            neo4j_user = arg
        elif opt in ("-p", "--neo4j-password"):
            neo4j_password = arg
        elif opt in ("-s", "--source-path"):
            source_dataset_path = arg
        elif opt in ("-b", "--bolt"):
            uri = arg
    print('Neo4j user is', neo4j_user)
    print('Neo4j password is', neo4j_password)
    print('Neo4j uri is', uri)
    if source_dataset_path:
        print('Source Dataset Path is', source_dataset_path)

    return neo4j_user, neo4j_password, source_dataset_path, uri, opts, args
