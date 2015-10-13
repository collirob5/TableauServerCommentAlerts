import psycopg2
import sys
import os
import getopt
import math
import json
import time
import datetime as dt
import smtplib
from email.mime.text import MIMEText

def getArgs(argv):
    arguments={}
    try:
        opts, args = getopt.getopt(argv,"hd:c:q:")
    except getopt.GetoptError:
        print '~~~~~~~~~ERROR~~~~~~~~~\nAn exception occurred mapping the arguments.\n'
        printHelp()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            printHelp()
            sys.exit(2)
        elif opt == '-d':
            arguments['debugFlag'] = int(arg)
        elif opt == '-q':
            arguments['query'] = arg
        elif opt == '-c':
            arguments['configFile'] = arg
    #Get the rest of the arguments out of the config file.
    arguments = parseConfig(arguments)

    if arguments['debugFlag'] >=2:
        print 'getArgs is returning:\n'
        print arguments
    return arguments

def parseConfig(args):
    #Open the config file and read in the contents.
    configFile = open(args['configFile'],'r')
    config = configFile.read()
    configMap = json.loads(config)
    configFile.close()

    #Content checking
    requiredFields = ['user','password','host','port','dbname','histFile','fromEmail','smtpHost']
    loop = True
    index = 0
    if 'debugFlag' in configMap and not 'debugFlag' in args:
        args['debugFlag'] = configMap['debugFlag']
    elif 'debugFlag' not in args:
        args['debugFlag'] = 0
    while loop == True:
        if not requiredFields:
            if args['debugFlag'] >= 3:
                print 'requiredFields is now empty.'
            loop = False
        elif index >= len(requiredFields):
            if args['debugFlag'] >= 3:
                print 'Index has exceeded requiredFields size.'
            loop = False
        elif requiredFields and requiredFields[index] in configMap:
            if args['debugFlag'] >= 3:
                print 'Argument ' + str(requiredFields[index]) + ' found.'
            del requiredFields[index]
        else:
            if args['debugFlag'] >= 3:
                print 'Missing required field: ' + requiredFields[index]
            index += 1
    if requiredFields:
        print '~~~~~~~~~ERROR~~~~~~~~~\nRequired argument(s) are missing in the script call: ' + str(requiredFields) + '\n'
        printHelp()
        sys.exit(2)
    #Assign json values to args map.
    args['dbhost'] = configMap['host']
    args['dbport'] = configMap['port']
    args['dbname'] = configMap['dbname']
    args['dbuser'] = configMap['user']
    args['dbpass'] = configMap['password']
    args['histFile'] = configMap['histFile']
    args['fromEmail'] = configMap['fromEmail']
    args['smtpHost'] = configMap['smtpHost']
    return args

def printHelp():
    print 'COMMAND LINE ARGUMENTS:' \
          '\n\tREQUIRED:' \
          '\n\t\t-c </path/to/file.json> --config file holding values listed below.'
    print '\n\tOPTIONAL:' \
          '\n\t\t-q <query string>  --Query to execute against the database.' \
          '\n\t\t-d <debugFlag 0-10> --integer value for debug purposes.  0 is off.' \
          '\n\t\t-h <help menu> --print this help menu.'
    print 'CONFIG FILE SETTINGS:' \
          '\n\tREQUIRED:' \
          '\n\t\thost: <fqdn or IP>  --host where postgres server exists' \
          '\n\t\tdbname: <database name>  --name of database to query' \
          '\n\t\tuser: <username>  --user that has read access to database' \
          '\n\t\tpassword: <password>  --password corresponding to username provided' \
          '\n\t\thistFile: </file.path>  --path to the history file listing previously sent comment IDs' \
          '\n\t\tfromEmail: <email>  --email address from which notifications should be sent.' \
          '\n\t\tsmtpHost: <fqdn or IP>  --host that relays SMTP traffic'
    print 'EXAMPLE:' \
          '\n\tpython /path/to/script.py -c "/path/to/config.json" -d 0'

def sendEmail(args,commentInfo):
    if args['debugFlag'] >= 3:
        print 'sendEmail received a comment for sending.'
    msgString = 'A comment was submitted by ' + commentInfo['commenterName'] + ' to the "' + commentInfo['viewName'] + '" view inside of the "' + commentInfo['workbookName'] + '" workbook owned by ' + commentInfo['ownerName'] + '.  The comment reads:\n\n' + commentInfo['commentText']
    msg = MIMEText(msgString)
    msgFrom = args['fromEmail']
    msgTo = [commentInfo['ownerEmail'], commentInfo['commenterEmail']]
    msg['Subject'] = "TABLEAU SERVER: New Comment in " + commentInfo['workbookName'] + ":" + commentInfo['viewName']
    msg['From'] = args['fromEmail']
    msg['To'] = ", ".join(msgTo)

    if args['debugFlag'] >= 3:
        print 'An email is being sent to ' + str(msg['To']) + ' regarding comment ' + str(commentInfo['id']) + '.'
    s = smtplib.SMTP(args['smtpHost'])
    s.sendmail(args['fromEmail'],msgTo,str(msg))
    s.quit()

def parseComments(args,rows):
    rowList = []
    comments = []
    comment = {}
    skipChars = [',','"','(',')',' ',"'"]
    fieldValue = ''
    i = 0
    quoteToggle = False
    for row in rows:
        if args['debugFlag'] >= 3:
            print 'Parsing row:\n' + str(row)
        comment['id'] = int(row[0])
        comment['commenterName'] = row[1]
        comment['commenterEmail'] = row[2]
        comment['commentText'] = row[3]
        comment['workbookName'] = row[4]
        comment['viewName'] = row[5]
        comment['ownerName'] = row[6]
        comment['ownerEmail'] = row[7]
        #Append object to list of all comments
        if args['debugFlag'] >= 3:
            print 'Adding comment to array:\n' + str(comment)
        comments.append(comment)
        #Reset Loop Variables
        quoteToggle = False
        i = 0
        rowList = []
        comment = {}
    return comments

def checkComments(args,comments):
    histFile = open(args['histFile'],'a+')
    oldComments = []
    for line in histFile:
        oldComments.append(int(line))
    if args['debugFlag'] >= 3:
        print 'oldComments consists of: ' + str(oldComments)
    for comment in comments:
        sent = False
        if args['debugFlag'] >= 3:
            print 'Current commentID: ' + str(comment['id'])
        if comment['id'] in oldComments:
            if args['debugFlag'] >= 3:
                print 'Skipping Comment ID ' + str(comment['id']) + ' because an alert was already sent for that comment.'
            continue
        sendEmail(args,comment)
        histFile.write(str(comment['id']) + '\n')

def main(argv):
    args = getArgs(argv)
    #Create a connection string
    connString = "host='" + args['dbhost'] +"' port='" + args['dbport'] + "' user='" + args['dbuser'] + "' password='" + args['dbpass'] + "' dbname='" + args['dbname'] + "'"
    #Attempt a database connection using the connection string created.
    try:
        if args['debugFlag'] >= 2:
            print 'Attempting database connection with the following string:\n' + connString
        conn = psycopg2.connect(connString)
    except:
        print 'Database connection failed.'
        sys.exit(2)
    if args['debugFlag'] >= 1:
        print 'Database connection successful.'

    #Create an action handler
    cur = conn.cursor()
    if 'query' in args:
        query = args['query']
    else:
        query = """select c.id,u.friendly_name,uv.email,c.comment,w.name,v.name,(select u2.friendly_name from _users u2 where u2.id=w.owner_id),(select uv2.email from users_view uv2 where uv2.id=w.owner_id) from comments c join users_view uv on uv.id = c.user_id join _users u on u.id=c.user_id join historical_events he on he.hist_comment_id=c.id join views v on he.hist_view_id=v.id join workbooks w on w.id = v.workbook_id"""
    
    #Execute our query
    if args['debugFlag'] >= 2:
        print 'Executing query: \n' + query
    cur.execute(query)
 

    #Get query results
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    if args['debugFlag'] >= 2:
        print 'Query returned ' + str(len(rows)) + ' rows.'
    if args['debugFlag'] >= 4:
        print 'Query Results:\n'
        print str(colnames)
        for row in rows:
            print row
    
    if not 'query' in args:
        #Parse results into an array of objects
        comments = parseComments(args,rows)

        #Check parsed comments against previous notifications.
        checkComments(args,comments)

if __name__ == '__main__':
    main(sys.argv[1:])
