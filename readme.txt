Gate2MySQL
Plugin to export annotations and features from corpora from Gate to Mysql.
Unpack this plugin in a folder and call it from within GATE Plugins interface
MySQL should be installed previously.
The Plugin contains the JDBC to access MysQL

Good Practice: Execute this plugin in a separate corpus pipeline after processing
your corpus in the main pipeline. It works also with a previously
stored Java Datastore
  
The parameters from creole.xml are:
servidor	= MySQL Server name
banco 		= MySQL Database Schema name (creates the DB schema when the plugin is loaded)
porta 		= Port of your MySQL Server
usuario		= MySQL Server Username
senha		= MySQL Server Password
   
  
Agnaldo L Martins (Doctorate Researcher,ECI/UFMG)
agnaldo@toptc.com.br
Renato Rocha Souza (Associate Researcher,FGV/RJ and ECI/UFMG)
renato.souza@fgv.br

2010
Universidade Federal de Minas Gerais (UFMG) - MG - Brazil
Fundação Getulio Vargas (FGV) - RJ - Brazil
