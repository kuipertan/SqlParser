
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class MySQLParser {

	public static void main(String[] args) {
		
		try {
			System.setOut(new PrintStream(new FileOutputStream("system_out.txt")));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  SqlStatistic st = new SqlStatistic();
		  File root = new File(args[0]);
		  File[] files = root.listFiles();
		  for(File file:files){     
		     if(file.isDirectory()){
		    	 continue;
		     }
		     
		     Long filelength = file.length();  
		     byte[] filecontent = new byte[filelength.intValue()];  
		     try {  
		          FileInputStream in = new FileInputStream(file);  
		          in.read(filecontent);  
		          in.close();  
		          String[] sql = new String(filecontent, "ISO-8859-1").split("rrrrrrrr");

		          if (sql.length == 3){
		        	  //doParse(java.net.URLDecoder.decode(sql[2], "utf-8"));		        	  
		        	  st.parse(java.net.URLDecoder.decode(sql[2], "utf-8"));
		          }
		     } catch (IOException e) {  
		          e.printStackTrace();  
		     }  
		 }		 
		
		  st.OutputResult();
	}
		// TODO Auto-generated method stub
	static void doParse(String sql){
		sql = sql.trim();
		//sql = "select a,b,c,t.d from mytab t ";
		String prefix = sql.substring(0, 6).toUpperCase(); // select
		if (!prefix.startsWith("SELECT"))
			return;
		CCJSqlParserManager pm = new CCJSqlParserManager();		
		
		//sql = "SELECT * FROM MY_TABLE1 join MY_TABLE2 on id=iid "+
		//" WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6) group by ustb,buaa" ;
		//sql = "select a, b * c as d , count(b) from mytab t where  month(year)=1 and id in (SELECT * FROM MY_TABLE6) and u=89 group by t.b";
		//sql = "select a from tab where a<'2016-03-03 16:53:00.000'";

		Statement statement = null;
		try {
			statement = pm.parse(new StringReader(sql));
		} catch (JSQLParserException e) {
		//	e.printStackTrace();
		}
		
		if (statement instanceof Select) {
	        Select sel = (Select) statement;
	        SelectBody selectBody = sel.getSelectBody();
	        
	        /*
	        if (selectBody instanceof Union) {
	            // dataStore.registerView(typeName, (Union) selectBody);
	        	//union.getPlainSelects().get(0)
	            throw new UnsupportedOperationException(
	                    "ArcSDEDataStore does not supports registering Union queries");
	        } else if (selectBody instanceof PlainSelect) {
	            return (PlainSelect) selectBody;
	        } else {
	            throw new IllegalStateException(selectBody.getClass().getName());
	        } */
	        
	        if (selectBody instanceof PlainSelect) {
	        	PlainSelect ps =  (PlainSelect) selectBody;
	        	System.out.println(ps.toString());
	        	FromItem f = ps.getFromItem();
	        	//net.sf.jsqlparser.schema.Table tab = (Table)f;
	        	System.out.println("From:" + f.getClass());
	        	if (f instanceof SubSelect){
	        		selectBody = ((SubSelect)f).getSelectBody();
	        		if (selectBody instanceof PlainSelect) {
	    	        	 ps =  (PlainSelect) selectBody;
	    	        	 f = ps.getFromItem();
	    	        	 System.out.println("From:" + f.getClass());
	        		}
	        	}
	        //	Alias a = f.getAlias();
	        //	System.out.println("Alias:" + a.getName());
	        	
	        	List<Join> jo = ps.getJoins();
	        	if (jo != null){
	        		System.out.println("Join:" + jo.iterator().next().getClass());
	        		for(int i = 0; i < jo.size(); i++)  {  
	    	            Join join = jo.get(i);  
	    	            System.out.println(join.getClass());  	    	            
	    	     }  
	        	}
	        	
	        	List<Expression> gpb = ps.getGroupByColumnReferences();
	        	if (gpb != null)
	        		System.out.println("group by:" + gpb.iterator().next().getClass());
	        	
	        	Expression wh = ps.getWhere();	  
	        	if (wh != null)
	        		System.out.println("where:" + wh.getClass());
	        	
	        	List<SelectItem> items = ps.getSelectItems();
	        	if (items != null )
	        		System.out.println("select items:" + items.iterator().next().getClass());
	        	
	        }
	        else{
	        	System.out.println("XXXXXXXXXXXXXXXXX selectBody is not instance of PlainSelect");
	        }
		}


	}

}
