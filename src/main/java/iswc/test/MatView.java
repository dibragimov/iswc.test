package iswc.test;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class MatView {
	private String query;
	private int number;
	private int cost;
	private String name;
	private String graph;
	private TupleExpr te;
	
	public MatView(String name, int number, String graph, int cost){
		this.name = name;
		this.cost = cost;
		this.graph = graph;
		this.number = number;
	}
	
	public String getQuery(){
		return query;
	}
	public void setQuery(String qry){
		query = qry;
		SPARQLParser sprqlQ = new SPARQLParser();
		try {
			ParsedQuery pquerMV = sprqlQ.parseQuery(this.query, "http://10.15.120.1:8890/sparql");
			te = pquerMV.getTupleExpr();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			te = null;
		}
	}
	
	public TupleExpr getTupleExpr(){
		return te;
	}
	
	public int getCost(){
		return cost;
	}
	public void setCost(int cost){
		this.cost = cost;
	}
	
	public String getGraph(){
		return graph;
	}
	public void setGraph(String graph){
		this.graph = graph;
	}

	public String getName(){
		return name;
	}
	public void setName(String name){
		this.name = name;
	}
	
	public int getNumber(){
		return number;
	}	
	public void setNumber(int number){
		this.number = number;
	}
}
