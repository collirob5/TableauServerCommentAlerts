# TableauServerCommentAlerts
This code queries a Tableau Server for comments and emails the owner and commenter when a comment is submitted.

**NOTE:**  Before Tableau database queries will work you must enable the external access functionality within Tableau Server.  See here:
http://onlinehelp.tableau.com/current/server/en-us/help.htm#adminview_postgres_access.htm

**---commentQuery.py---**
Primary script.  Must call with command line flag '-c' followed by config file path.  Other flags available.  Use '-h' for help.

**---config.json---**
Configuration file.  Default values for required fields provided.  Run 'commentQuery.py' -h for list of requirements.

##EXAMPLE USAGE

####python /scripts/commentQuery.py -c "/path/to/config.json"
----This would be a standard no-debug execution of the comment Query script
  
####python /scripts/commentQuery.py -h
----Print help

####python /scripts/commentQuery.py -c "/path/to/config.json" -q "SELECT * FROM COMMENTS"
----Manual execution of a query specified at the command line.

####python /scripts/commentQuery.py -c "/path/to/config.json" -d 3
----Standard execution with 'level 3' debugging.  Debugging levels are subjective by author.
