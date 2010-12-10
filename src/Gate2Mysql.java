
/*
 *  Gate2Mysql.java
 *
 *
 *  Copyright (c) 2010, Universidade Federal de Minas Gerais.
 *  Plugin to export data from Gate to Mysql.
 *  
 *  MySQL should be installed previously.
 *  Good Practice: Execute this plugin in a separate corpus pipeline after processing
 *  your corpus in the main pipeline. It works also with a previously
 *  stored Java Datastore
 *  
 *  The parameters from creole.xml are:
 *  servidor            = MySQL Server name
 *  banco		= MySQL Database Schema name (creates the DB schema when the plugin is loaded)
 *  porta		= Port of your MySQL Server
 *  usuario		= MySQL Server Username
 *  senha		= MySQL Server Password
 *  
 *  MYSQL: JDBC for the Mysql Connection is in Gate2Mysql.
 *    
 *  Creators:   Agnaldo L Martins (Doctorate Researcher,ECI/UFMG)
 *  		agnaldo@toptc.com.br
 *              Renato Rocha Souza (Researcher,FGV/RJ and ECI/UFMG)
 *  		renato.souza@fgv.br
 *  
 * 2010
 * Universidade Federal de Minas Gerais - MG - Brazil
 * Fundação Getúlio Vargas - RJ - Brazil
 */

import gate.Annotation;
import gate.AnnotationSet;
import gate.Corpus;
import gate.CorpusController;
import gate.Factory;
import gate.FeatureMap;
import gate.Gate;
import gate.LanguageAnalyser;
import gate.ProcessingResource;
import gate.Resource;
import gate.SimpleAnnotation;
import gate.SimpleAnnotationSet;
import gate.annotation.AnnotationImpl;
import gate.creole.ANNIEConstants;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.AbstractResource;
import gate.creole.ResourceInstantiationException;
import gate.creole.SerialAnalyserController;
import gate.creole.metadata.CreoleResource;
import gate.util.Benchmark;
import gate.util.IdBearer;
import gate.util.Out;
import gate.corpora.CorpusImpl;
import gate.corpora.DocumentImpl;
import gate.Document;


import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;


@SuppressWarnings("serial")
 
public class Gate2Mysql extends AbstractLanguageAnalyser implements LanguageAnalyser {

	//MySQL Server Address
	String servidor;
	
	//MySQL Database Schema to be created
	String banco;
	
	//MySQL Server Port - (default is 3306)
	String porta;
	
	//User name for connection to MySQL Database
	String usuario;
	
	//Password
	String senha;
	
	public int processo;
	
	public String getservidor() {return this.servidor;}
	public void setservidor(String servidor) {this.servidor=servidor;}
	
	public String getporta() {return this.porta;}
	public void setporta(String porta) { this.porta=porta;}
	
	public String getbanco() {return this.banco;}
	public  void setbanco(String banco) {this.banco= banco;}
	
	public String getusuario() {return this.usuario;}
	public void setusuario(String usuario) {this.usuario=usuario;}
		
	public String getsenha() {return this.senha;}
	public void setsenha(String senha) {this.senha=senha;}
	
	
	
	    
	public Resource init () throws ResourceInstantiationException 
	{
		System.out.println("Gate2Mysql - Started");
		System.out.println("CAUTION: If you are willing to run your Gate2MySQL pipeline more than once, reinitialise the Plugin.");
		System.out.println("         Click with right buton in the Gate2Mysql, and then press Reinitialize.");
		
		//Controls the creation of the DB Schema only in the first document
		processo=0;
		
		//Connect to the MySQL database
		Connection conn = conectaservidor(servidor,  usuario, senha);
		
		//Creating the DB schema in the first run
		System.out.println("Creating database schema" +banco + "...");
		criabanco(conn,getbanco());
		
		return super.init();	
	}
	
	public void reInit() throws ResourceInstantiationException	  {
		 init();   }
	
	
	public void execute () 
	{
		/*Caution: The "processo" variable is a counter for the number of docs in the corpus.
		During Gate2Mysql pipeline processing for the first doc, ALL DOCS ARE PROCESSED .
		In case of many documentss, in the first doc processed, all others docs are processed too.*/
		
		processo++;
		//Controls the creation of the DB schema only in the first document
		if(processo==1)
		{
			System.out.println("Starting database insert...");
			
			//Connecting to the database
			Connection conn = conectaservidor(servidor,  usuario, senha);

			System.out.println("Begginning export of annotations for " +corpus.size()+ " documents...");
			
			//Iterating through all the documents of the corpus
			for(int docs=0;docs<corpus.size();docs++)
			{
		
				//Get annotations for one document
				DocumentImpl doc = (DocumentImpl)corpus.get(docs);
				AnnotationSet annotations = doc.getAnnotations();

				ArrayList<Annotation> annots = new ArrayList<Annotation>(annotations);
				Iterator<String> anotacoes = annotations.getAllTypes().iterator();
		
				//Traverses the list of types of annotations found in the document
				while(anotacoes.hasNext()) 
				{
					String nomedatabela = (String)anotacoes.next();
					AnnotationSet marcacoes = doc.getAnnotations();
					 
					//On each annotation found, check which features exist
					String camposencontrados 	= getcampos(annots,nomedatabela).toString();
					camposencontrados 		= camposencontrados.replace("[", "");
					camposencontrados 		= camposencontrados.replace("]", "");

					//Adding additional fields for table creation
					String campos="`documentname` VARCHAR(256) ,`id` VARCHAR(256) ,`start` VARCHAR(256) ,`end` VARCHAR(256) ";
					
					if(!camposencontrados.isEmpty())
						campos = campos +","+camposencontrados;
					
					//Create the table with features and additional fields
					criatabela( conn,  getbanco(),  nomedatabela,  campos);
						    
				}
				
	
				//Retraces the notes, now to insert the data in database
				for(int i=0; i<annots.size(); i++) 
				{
					
					AnnotationImpl teste = (AnnotationImpl) annots.get(i);
					
					String documento 	= doc.getName();
					String gateid		= teste.getId().toString();
					String termino 		= teste.getEndNode().getOffset().toString();
					String start 		= teste.getStartNode().getOffset().toString();
					String tipo 		= teste.getType().toString();
					String concept 		= teste.getFeatures().toString();

					Iterator featureIterator = teste.getFeatures().keySet().iterator();
					String listadecampos 	= "`documentname`,`start`,`id`,`end`";
					String listadevalores 	= "'"+documento+"','"+start+"','" + gateid +"','" + termino+"'";
					
					while(featureIterator.hasNext()) 
					{
						String key = (String)featureIterator.next();
						listadecampos  = listadecampos+",`"+key+"`";
						listadevalores = listadevalores +",'"+teste.getFeatures().get(key).toString().replaceAll("'", "`")+"'";
				
					}
					
					//Insert all data in the table
					inseredados( conn,  banco,  tipo,  listadecampos,  listadevalores );
					
					
				}	
				
			}//Next document in corpus

		}
	    
	}
	
	
	//CONNECT INTO MYSQL
	public Connection conectaservidor(String servidor, String usuario, String senha)
	{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

	    Connection conn = null;
		try {
			conn = DriverManager.getConnection(servidor ,usuario, senha);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    //System.out.println("Successfully established connection!"); 
	    return conn;
	}
	
	
	//CREATE DATABASE SCHEMA
	public void criabanco(Connection conn, String nomedobanco)
	{
		nomedobanco=nomedobanco.trim().toLowerCase();
		String comandoSql ="DROP DATABASE IF EXISTS " + nomedobanco;  
		try {
			PreparedStatement ps = conn.prepareStatement(comandoSql);
	        ps.executeUpdate(); 
	        
		} catch (SQLException e) {
			System.out.println("Problems in the database delete/n");
			e.printStackTrace();
		}  
				
		comandoSql ="CREATE DATABASE " + nomedobanco;  
		try {
			PreparedStatement ps = conn.prepareStatement(comandoSql);
	        ps.executeUpdate(); 
	        
		} catch (SQLException e) {
			System.out.println("Problems in the database create/n");
			e.printStackTrace();
		}  
		System.out.println("Successfully database create!\n");
	}

	//CREATE TABLE IN DATABASE SCHEMA
	public void criatabela(Connection conn, String nomedobanco, String nomedatabela, String nomedoscampos)
	{
		//Remove all spaces in the name of database schema e tables, and convert to lowercase
		nomedobanco 			= nomedobanco.trim().toLowerCase();
		nomedatabela 			= nomedatabela.trim().toLowerCase();
		String todososcampos	= nomedoscampos;
		
		//Create table if it does not exist. CAUTION: Using utf8 as type is recommended
		String comandoSql ="CREATE TABLE IF NOT EXISTS " + nomedobanco +"." + nomedatabela + "("+ todososcampos + ") ENGINE = InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci;";  
		
		if(nomedatabela.length()>0)
		{
			try {
				PreparedStatement ps = conn.prepareStatement(comandoSql);
				ps.executeUpdate(); 
	        
			} catch (SQLException e) {
				System.out.println("Problems in table create");
				e.printStackTrace();
			}
		}	
        
	}

	//INSERT DATA IN THE TABLE
	public void inseredados(Connection conn, String banco, String tabela, String campos, String dados)
	{
		//Remove all spaces in the name of database schema e tables, and convert to lowercase
		banco 	= banco.trim().toLowerCase();
		tabela 	= tabela.trim().toLowerCase();
		campos 	= campos.trim().toLowerCase();
		
		//Insert the data in the table
		String comandoSql = "INSERT INTO " + banco + "." + tabela + "(" + campos + ")" + " VALUES (" + dados +")";
		
		try {
            Statement stm = conn.createStatement();  
            stm.executeUpdate(comandoSql);
	        
		} catch (SQLException e) {
			System.out.println("Problems in the insert command, table: "+ tabela );
			System.out.println("Command sql: " + comandoSql);
			
			e.printStackTrace();
		}  
	}
	
	//CREATING METADATA IN TABLE
	public void criacampo(Connection conn, String nomedobanco, String nomedatabela, String nomedocampo)
	{
		//Remove all spaces in the name of database e tables, and convert to lowercase
		nomedobanco 	= nomedobanco.trim().toLowerCase();
		nomedatabela 	= nomedatabela.trim().toLowerCase();
		nomedocampo 	= nomedocampo.trim().toLowerCase();
		
		//Before attempting to include field checks whether it already exists on this table
		String comandoSql = "SELECT * FROM information_schema.COLUMNS WHERE COLUMN_NAME="+nomedocampo+" AND TABLE_NAME="+nomedatabela+" AND TABLE_SCHEMA=" + nomedobanco;
	        
		try 
		{
			Statement st = conn.createStatement();
			ResultSet rs = null;

			rs = st.executeQuery(comandoSql);
			if(!rs.next())
			{
				comandoSql ="ALTER TABLE " + nomedobanco +"." + nomedatabela + " ADD COLUMN "+ nomedocampo + " VARCHAR(256)";  
		
				try {
					PreparedStatement ps = conn.prepareStatement(comandoSql);
					ps.executeUpdate(); 
	        
				} catch (SQLException e) {
					System.out.println("Problems inserting metadata "+ nomedocampo + " in table " + nomedatabela);
					e.printStackTrace();
				}
			}
		} catch (SQLException e) {
        e.printStackTrace();
        }
	
	}

	//GET ALL METADATA IN THE FEATURE
	public Vector getcampos (ArrayList<Annotation> annots, String tipodeanotacao)
	{

		
		Vector resultado = new Vector();
		Vector controle = new Vector();
		
		for (int p = 0; p<annots.size();p++)
		{
		
			if ( annots.get(p)!=null)
			{
				if(annots.get(p).getType().toLowerCase().equals(tipodeanotacao.trim().toLowerCase())==true)
				{
					
					String 	concept 	= annots.get(p).getFeatures().toString();
					
					Iterator featureIterator = annots.get(p).getFeatures().keySet().iterator();
	
					while(featureIterator.hasNext()) 
					{
						int adicionado=0;
						String key = (String)featureIterator.next();

						for(int ve=0;ve<resultado.size();ve++)
						{
							if( controle.elementAt(ve)==key)
								adicionado=1;
						}
						if(adicionado==0)
						{controle.add(key);
							resultado.add("`" +key + "` VARCHAR(256) ");
						}
						   
						
					}	
				}
				
								
			}

		}
		return resultado;
	}

}
