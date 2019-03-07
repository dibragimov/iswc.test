package iswc.test;

import java.util.List;

import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.StatementPattern;

public class AggregateInfo {
	private List<String> predicates;
	private List<String> variables;
	private String nameVar;
	private List<StatementPattern> patterns;
	private ExtensionElem function; 
	
	public AggregateInfo(){
		
	}
	
	public AggregateInfo(String nameVar, List<String> variables, List<String> predicates, List<StatementPattern> patterns, ExtensionElem element){
		this.nameVar = nameVar;
		this.predicates = predicates;
		this.patterns = patterns;
		this.variables = variables;
		this.function  = element;
	}
	
	public List<String> getPredicates() {
		return predicates;
	}
	public void setPredicates(List<String> predicates) {
		this.predicates = predicates;
	}
	public List<StatementPattern> getPatterns() {
		return patterns;
	}
	public void setPatterns(List<StatementPattern> patterns) {
		this.patterns = patterns;
	}
	public ExtensionElem getFunction() {
		return function;
	}
	public void setFunction(ExtensionElem function) {
		this.function = function;
	}
	public List<String> getVariables() {
		return variables;
	}
	public void setVariables(List<String> variables) {
		this.variables = variables;
	}
	public String getNameVar() {
		return nameVar;
	}
	public void setNameVar(String nameVar) {
		this.nameVar = nameVar;
	}
}
