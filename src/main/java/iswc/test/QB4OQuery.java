package iswc.test;

public class QB4OQuery {
	private String query;
	private int number;
	private String name;
	
	public QB4OQuery(String name, int number){
		this.name = name;
		this.number = number;
	}
	
	public String getQuery(){
		return query;
	}
	public void setQuery(String qry){
		query = qry;
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
