package iswc.test;

import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

public class DimensionInfo {
	private final String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
	private final String prefix_qb = "http://www.w3.org/2004/02/skos/core#narrower";
	private String predicate;
	private List<StatementPattern> patterns;
	private boolean isKept;
	private String groupByVar;
	private String filterVar;
	private String selectedDimVar;
	private StatementPattern firstPattern;
	
	public DimensionInfo(){
		
	}
	
	public DimensionInfo(String predicate, List<StatementPattern> patterns, StatementPattern firstPattern, boolean isKept, String groupByVar, String filterVar){
		this.filterVar = filterVar;
		this.predicate = predicate;
		this.patterns = patterns;
		this.groupByVar = groupByVar;
		this.firstPattern = firstPattern;
		this.isKept = isKept;
	}
	
	public String getPredicate() {
		return predicate;
	}
	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}
	public List<StatementPattern> getPatterns() {
		return patterns;
	}
	public void setPatterns(List<StatementPattern> patterns) {
		this.patterns = patterns;
	}
	public boolean isKept() {
		return isKept;
	}
	public void setKept(boolean isKept) {
		this.isKept = isKept;
	}
	public String getGroupByVar() {
		return groupByVar;
	}
	public void setGroupByVar(String groupByVar) {
		this.groupByVar = groupByVar;
	}
	public String getFilterVar() {
		return filterVar;
	}
	public void setFilterVar(String filterVar) {
		this.filterVar = filterVar;
	}
	public String getSelectedDimVar() {
		if(selectedDimVar == null){
			if(filterVar == null && groupByVar != null)
				selectedDimVar = groupByVar;
			else if(filterVar != null && groupByVar == null)
				selectedDimVar = filterVar;
			else if(filterVar != null && groupByVar != null){
				for (StatementPattern triple : patterns) {
					if((triple.getSubjectVar().getName().contains(filterVar) || triple.getSubjectVar().getName().contains(groupByVar)) 
							&& triple.getPredicateVar().getValue().stringValue().equals(prefix_qb4o)){
						selectedDimVar = triple.getSubjectVar().getName();
						break;
					}
					else if((triple.getObjectVar().getName().contains(filterVar) || triple.getObjectVar().getName().contains(groupByVar)) 
							&& triple.getPredicateVar().getValue().stringValue().equals(prefix_qb)){
						selectedDimVar = triple.getObjectVar().getName();
						break;
					}
				}
			}
		}
		return selectedDimVar;
	}
	public StatementPattern getFirstPattern() {
		return firstPattern;
	}
	public void setFirstPattern(StatementPattern firstPattern) {
		this.firstPattern = firstPattern;
	}
}
