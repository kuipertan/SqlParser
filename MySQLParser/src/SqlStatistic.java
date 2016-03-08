import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.*;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;



public class SqlStatistic {
	
	public HashMap<String,Long> funcs = new HashMap<String,Long>();
	public HashMap<String,Long> gpb = new HashMap<String,Long>();
	public HashMap<String,ArrayList<String>> keySql = new HashMap<String,ArrayList<String>>();
	public HashMap<String, HashMap<String, Integer> > joins = new HashMap<String, HashMap<String, Integer> > ();

	private CCJSqlParserManager pm = new CCJSqlParserManager();
	
	public void OutputResult(){
		
		System.out.println("********Group by statistic********");
		List<Map.Entry<String, Long>> ordered =
			    new ArrayList<Map.Entry<String, Long>>(gpb.entrySet());
		Collections.sort(ordered, new Comparator<Map.Entry<String, Long>>() {   
		    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {      
		        //return (o2.getValue() - o1.getValue()); 
		        return (int)(o2.getValue() - o1.getValue());
		    }
		}); 
		for (int i = 0; i < ordered.size(); i++) {
		    Map.Entry<String, Long> v = ordered.get(i);
		    System.out.println(v.getKey() + ", " + v.getValue());		    
		}
		
		System.out.println("\n********Functions statistic********");
		List<Map.Entry<String, Long>> orderedFuncs =
			    new ArrayList<Map.Entry<String, Long>>(funcs.entrySet());
		Collections.sort(orderedFuncs, new Comparator<Map.Entry<String, Long>>() {   
		    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {      
		        //return (o2.getValue() - o1.getValue()); 
		        return (int)(o2.getValue() - o1.getValue());
		    }
		}); 
		for (int i = 0; i < orderedFuncs.size(); i++) {
		    Map.Entry<String, Long> v = orderedFuncs.get(i);
		    System.out.println(v.getKey() + ", " + v.getValue());		    
		}
		
		
		System.out.println("\n********Join statistic********");
		List<Map.Entry<String, HashMap<String, Integer>>> orderedJoin =
			    new ArrayList<Map.Entry<String, HashMap<String, Integer>>>(joins.entrySet());
		Collections.sort(orderedJoin, new Comparator<Map.Entry<String, HashMap<String, Integer>>>() {   
		    public int compare(Map.Entry<String, HashMap<String, Integer>> o1, Map.Entry<String, HashMap<String, Integer>> o2) {      
		        //return (o2.getValue() - o1.getValue()); 
		        return (int)(o2.getValue().size() - o1.getValue().size());
		    }
		}); 
		for (int i = 0; i < orderedJoin.size(); i++) {
			Map.Entry<String, HashMap<String, Integer>> v = orderedJoin.get(i);
		    //System.out.println(v.getKey() + ", " + v.getValue());	
		    String jointabs = v.getKey();
		    System.out.print(v.getKey());		    
		    HashMap<String, Integer> vv = v.getValue();
		    Iterator iter = vv.entrySet().iterator();
		    while (iter.hasNext()) {
		    	Map.Entry entry = (Map.Entry) iter.next();
		    	System.out.print("\n\t" +entry.getKey());
		    }
		    System.out.println();
		}
		
		
		System.out.println("\n********Where condition statistic********");
		List<Map.Entry<String,ArrayList<String>>> wh =
			    new ArrayList<Map.Entry<String,ArrayList<String>>>(keySql.entrySet());
		Collections.sort(wh, new Comparator<Map.Entry<String,ArrayList<String>>>() {   
		    public int compare(Map.Entry<String,ArrayList<String>> o1, Map.Entry<String,ArrayList<String>> o2) {      
		        //return (o2.getValue() - o1.getValue()); 
		        return (int)(o2.getValue().size() - o1.getValue().size());
		    }
		}); 
		for (int i = 0; i < wh.size(); i++) {
		    Map.Entry<String,ArrayList<String>> v = wh.get(i);
		    ArrayList<String> sqls = v.getValue();
		    System.out.print(v.getKey() + " used times:" + sqls.size());
		    for( int j=0; j < sqls.size(); j++){
		    	System.out.print("\n\t," + sqls.get(j).replace("\n", ""));		    	
		    }
		    System.out.println();
		}
	}
	
	public void parse(String sql){
		sql = sql.trim();
		//sql = "select a,b,c,t.d from mytab t ";
		String prefix = sql.substring(0, 6).toUpperCase(); // select
		if (!prefix.startsWith("SELECT"))
			return;
		
		Statement statement = null;
		try {
			statement = pm.parse(new StringReader(sql));			

		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return;
		}
				
		if (statement instanceof Select) {
			Select sel = (Select) statement;
	        SelectBody selectBody = sel.getSelectBody();
	        if (!(selectBody instanceof PlainSelect)) {
	        	return;
	        }        
	     	parsePlainSelect((PlainSelect) selectBody, sql);        
		}		
	}
	
	private void parsePlainSelect(PlainSelect ps, String currentSql){
		// Get table name firstly
        FromItem f = ps.getFromItem();
    	String fromTabName = "";
    	String fromTabAlias = null;
    	HashMap<String,String> tabNameMap = new HashMap<String,String>();
    	if( f instanceof Table){
    		fromTabName = ((Table)f).getName();  
    		if (f.getAlias() != null){
    			fromTabAlias = f.getAlias().getName();
    			tabNameMap.put(fromTabAlias, fromTabName);
    		}
    	}
    	else if (f instanceof SubSelect){
    		SelectBody sub = ((SubSelect)f).getSelectBody();
    		if (sub instanceof PlainSelect)
    			parsePlainSelect((PlainSelect) sub, sub.toString());
    	}
    	
    	String allTabs = fromTabName;
    	// Secondly if joins    	
    	List<Join> sqljoin = ps.getJoins();
    	if (sqljoin != null){
    		//System.out.println("Join:" + jo.iterator().next().getClass());
    		 for(int i = 0; i < sqljoin.size(); i++)  {  
    	            Join join = sqljoin.get(i);  
    	            //System.out.println(list.get(i));  
    	            String joinTab = parseJoin(fromTabName, fromTabAlias, join, tabNameMap);
    	            if (joinTab != null)
    	            	allTabs = allTabs + "," + joinTab;
    	     }  
    		 allTabs = "(" + allTabs + ")";
    	}
    	
    	
    	List<Expression> gpbs = ps.getGroupByColumnReferences();
    	if (gpbs != null){
    		for(int i = 0; i < gpbs.size(); i++)  {  
	            Expression gp = gpbs.get(i);  	            
	            if (gp instanceof Column){
	            	Column col = (Column) gp;
	            	String colName = col.getColumnName();
	            	Table t = col.getTable();
	            	String name = t.getName();
	            	if (name != null) {
	            		if (tabNameMap.containsKey(name))
	            			name = tabNameMap.get(name);
	            	}
	            	else if (sqljoin == null){ // select from single table
	            		name = fromTabName;
	            	}
	            	else
	            		name = allTabs;
	            	
	            	String k = name + "." + colName;
	            	if (name == "")
	            		System.out.println(name);
	            	if (gpb.containsKey(k)){
	            		Long v = gpb.get(k);
	            		v += 1;
	            		gpb.put(k, v);
	            	}else{
	            		gpb.put(k, new Long(1));
	            	}
	            }
    		}  
    	}
    	
    	Expression wh = ps.getWhere();	  
    	if (wh != null){
    		parseExpression(wh, fromTabName, tabNameMap,allTabs,currentSql);
    	}
    	
    	List<SelectItem> items = ps.getSelectItems();
    	if (items != null ){
    		for(int i = 0; i < items.size(); i++)  { 
    			SelectItem ii = items.get(i);
    			if (ii instanceof SelectExpressionItem){
    				Expression epr  = ((SelectExpressionItem) ii).getExpression();
    				parseSelectItem(epr);
    			}
    		}
    	}
	}
	
	private String parseJoin(String tab, String alias, Join join, HashMap<String,String> nameMap){
		FromItem f = join.getRightItem();
		String ret = null;
		if(f instanceof SubSelect){
			SelectBody sub = ((SubSelect)f).getSelectBody();
    		if (sub instanceof PlainSelect)
    			parsePlainSelect((PlainSelect) sub, sub.toString());
		}
		else if (f instanceof Table){
			String name = ((Table)f).getName();  
    		if (f.getAlias() != null){   			
    			nameMap.put(f.getAlias().getName(), name);
    		}
    		
    		String key = tab + "~" + name;
    		HashMap<String,Integer> v;
    		if (joins.containsKey(key)){
    			v = joins.get(key);
    		}
    		else{
    			v = new HashMap<String,Integer>();
    		}
    		
    		Expression expr = join.getOnExpression(); 
    		if (expr != null){
    			String joinCols = parseJoinExpression(expr,tab, name, nameMap);
    			Integer count;
    			if (v.containsKey(joinCols)){
    				count = v.get(joinCols);
    				count += 1;
    			}else{
    				count = new Integer(1);
    			}
    			v.put(joinCols, count);
    		}
    		joins.put(key, v);
    		
    		ret = name;
		}
		
		return ret;
	}
	
	private String parseJoinExpression(Expression expr, String fromTab, String joinTab,HashMap<String,String> nameMap){
		
		String ret = "";
		if(expr instanceof BinaryExpression){
			BinaryExpression bexp = (BinaryExpression)expr;
			ret = parseJoinExpression(bexp.getLeftExpression(), fromTab, joinTab, nameMap);
			return ret + "," + parseJoinExpression(bexp.getRightExpression(), fromTab, joinTab, nameMap);
		}
		else if (expr instanceof Column){
			Column col = (Column) expr;
        	String colName = col.getColumnName();
        	Table t = col.getTable();
        	String name = t.getName();
        	if (name != null) {
        		if (nameMap.containsKey(name))
        			name = nameMap.get(name);
        		ret = name + "." + colName;
        	}
        	else
        		ret = colName;
        	return ret;
		}
		return ret;
	}
	
	private void parseExpression(Expression expr, String fromTab, HashMap<String,String> nameMap, String allTabs, String currentSql){
		
		if(expr instanceof BinaryExpression){
			BinaryExpression bexp = (BinaryExpression)expr;
			parseExpression(bexp.getLeftExpression(), fromTab, nameMap, allTabs, currentSql);
			parseExpression(bexp.getRightExpression(), fromTab, nameMap, allTabs, currentSql);
		}
		else if (expr instanceof Column){
			Column col = (Column) expr;
        	String colName = col.getColumnName();
        	Table t = col.getTable();
        	String name = t.getName();
        	if (name != null) {
        		if (nameMap.containsKey(name))
        			name = nameMap.get(name);
        	}
        	else if (nameMap.size() < 2){ // select from single table
        		name = fromTab;
        	}
        	else
        		name = allTabs;
        	
        	String k = name + "." + colName;
        	if (currentSql.length() < 20)
        		System.out.println("haha");
        	
        	if (keySql.containsKey(k)){
        		ArrayList<String> v = keySql.get(k);
        		v.add(currentSql);
        		keySql.put(k, v);
        	}else{
        		ArrayList<String> v = new ArrayList<String>();
        		v.add(currentSql);
        		keySql.put(k, v);
        	}
		}
		else if (expr instanceof InExpression){
			InExpression inexp = (InExpression)expr;
			parseExpression(inexp.getLeftExpression(), fromTab, nameMap, allTabs, currentSql);
			if (inexp.getLeftItemsList() instanceof SubSelect){
				SelectBody sub = ((SubSelect)inexp.getLeftItemsList()).getSelectBody();
	    		if (sub instanceof PlainSelect)
	    			parsePlainSelect((PlainSelect) sub, sub.toString());
			}
				
		}
		else if (expr instanceof Function){
			String func = ((Function)expr).getName();
			Long v;
			if (funcs.containsKey(func)){
				v = funcs.get(func);
				v += 1;
			}else {
				v = new Long(1);
			}
			funcs.put(func, v);
		}
	}
	
	private void parseSelectItem(Expression expr){		
		if(expr instanceof BinaryExpression){
			BinaryExpression bexp = (BinaryExpression)expr;
			parseSelectItem(bexp.getLeftExpression());
			parseSelectItem(bexp.getRightExpression());
		}
		
		else if (expr instanceof Function){
			String func = ((Function)expr).getName();
			Long v;
			if (funcs.containsKey(func)){
				v = funcs.get(func);
				v += 1;
			}else {
				v = new Long(1);
			}
			funcs.put(func, v);
		}
	}
}
