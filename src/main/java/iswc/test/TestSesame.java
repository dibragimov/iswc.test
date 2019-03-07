package iswc.test;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import info.aduna.iteration.Iterations;

import org.omg.CORBA.portable.ValueOutputStream;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.BinaryValueOperator;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.sparql.query.SPARQLTupleQuery;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;
import org.openrdf.sail.memory.MemoryStore;

import com.github.jsonldjava.utils.Obj;

public class TestSesame {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//test();
		//testVirt();
		testQuery();
	}

	public static void test(){
		Repository rep = new SailRepository(new MemoryStore());
		try {
			rep.initialize();
			RepositoryConnection conn = rep.getConnection();
			
			String namespace = "http://example.org/";

			ValueFactory f  = rep.getValueFactory();
			org.openrdf.model.URI john = f.createURI(namespace, "john");
			try{
				conn.add(john, RDF.TYPE, FOAF.PERSON);
				conn.add(john, RDFS.LABEL, f.createLiteral("John", XMLSchema.STRING));
				
				RepositoryResult<Statement> statements =  conn.getStatements(null, null, null, true);
				
				Model model = Iterations.addAll(statements, new LinkedHashModel());
				
				model.setNamespace("rdf", RDF.NAMESPACE);
				model.setNamespace("rdfs", RDFS.NAMESPACE);
				model.setNamespace("xsd", XMLSchema.NAMESPACE);
				model.setNamespace("foaf", FOAF.NAMESPACE);
				model.setNamespace("ex", namespace);
				
				Rio.write(model, System.out, RDFFormat.TURTLE);
			} 
			finally {
				conn.close();
			}
			
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		String matView4 = "prefix xsd: <http://www.w3.org/2001/XMLSchema#> \n"+
				"prefix qb: <http://purl.org/linked-data/cube#>\n"+ 
				"prefix skos: <http://www.w3.org/2004/02/skos/core#>\n"+ 
				"prefix qb4o: <http://purl.org/qb4olap/cubes#> \n"+
				"prefix ssb: <http://ssb.org/ssb#> \n"+
				"prefix ssb-inst: <http://ssb.org/ssb-inst#>\n"+
				"SELECT ?id ?mfgr ?c_city ?s_nation ?year (SUM(?revenue) AS ?sum_rev) \n"+
"  (COUNT(?revenue) AS ?c_revenue) (SUM(?revenue - ?supplycost) AS ?sum_profit) \n"+
"  (SUM(?extendedprice * ?discount) AS ?poss_revenue) \n"+
" WHERE { \n"+
"  ?li a qb:Observation ; \n"+
"   qb:dataSet ssb-inst:dataset1 ; \n"+
"   ssb:Lineorder_Part ?part ; \n"+
"    ssb:Lineorder_OrderDate ?lo_orderdate ; \n"+
"    ssb:Lineorder_Customer ?customer ; \n"+
"    ssb:Lineorder_Supplier ?supplier  ; \n"+
"    ssb:Lineorder_Revenue ?revenue ; \n"+
"    ssb:Lineorder_SupplyCost ?supplycost ; \n"+
"    ssb:Lineorder_ExtendedPrice ?extendedprice ; \n"+
"    ssb:Lineorder_Discount ?discount . \n"+
"    ?customer skos:broader ?c_city . \n"+
"    ?c_city qb4o:inLevel ssb:City . \n"+
"    ?supplier skos:broader ?s_city . \n"+
"    ?s_city qb4o:inLevel ssb:City . \n"+
"	   ?s_city skos:broader ?s_nation .\n"+
"	   ?s_nation qb4o:inLevel ssb:Nation .\n"+
"    ?part skos:broader ?brand . \n"+
"    ?part qb4o:inLevel ssb:Brand . \n"+
"    ?brand skos:broader ?category . \n"+
"    ?category qb4o:inLevel ssb:Category . \n"+
"    ?category skos:broader ?mfgr . \n"+
"    ?mfgr qb4o:inLevel ssb:Manufacturer . \n"+
"    ?lo_orderdate skos:broader ?month . \n"+
"    ?month qb4o:inLevel ssb:Month . \n"+
"    ?month skos:broader ?year . \n"+
"    ?year qb4o:inLevel ssb:Year . \n"+
"    BIND(IRI(CONCAT('http://ssb.org/ssb-inst#', STRAFTER(str(?c_city), 'http://ssb.org/ssb-inst#'), \n"+ 
"    STRAFTER(str(?s_city), 'http://ssb.org/ssb-inst#'), STRAFTER(str(?year), 'http://ssb.org/ssb-inst#'), \n"+ 
"    STRAFTER(str(?mfgr), 'http://ssb.org/ssb-inst#'))) AS ?id) . \n"+
"} \n"+
"GROUP BY ?id ?mfgr ?c_city ?s_nation ?year";
	}

	public static void testVirt(){
		///
		Repository rep = new SPARQLRepository("http://10.15.120.1:8890/sparql");
		long startTime = System.nanoTime();
		try {
			List<MatView> mvs = new MatViewsLoader().LoadMatViews();
			long endTime = System.nanoTime();
			long duration = (endTime - startTime);////in ms
			System.out.println(String.format("Duration %s, %s ms", duration, ((double)duration/1000000)));
			List<QB4OQuery> qries = new MatViewsLoader().LoadQueriess();
			
			rep.initialize();
			RepositoryConnection conn = rep.getConnection();
			String query = "prefix xsd: <http://www.w3.org/2001/XMLSchema#> \n"+
"prefix qb: <http://purl.org/linked-data/cube#>\n"+ 
"prefix skos: <http://www.w3.org/2004/02/skos/core#>\n"+ 
"prefix qb4o: <http://purl.org/qb4olap/cubes#> \n"+
"prefix ssb: <http://ssb.org/ssb#> \n"+
"prefix ssb-inst: <http://ssb.org/ssb-inst#>\n"+
"SELECT ?c_city ?s_city ?d_year (SUM(?revenue) AS ?lo_revenue) (COUNT(?revenue) AS ?lo_revenue_count)\n"+ 
"FROM <http://ssb1_qb4o.org>\n"+
"WHERE {\n"+
"    ?li a qb:Observation ;\n"+
"	   qb:dataSet ssb-inst:dataset1 ;\n"+
"	   ssb:Lineorder_OrderDate ?lo_orderdate ;\n"+
"	   ssb:Lineorder_Customer ?customer ;\n"+
"	   ssb:Lineorder_Supplier ?supplier  ;\n"+
"	   ssb:Lineorder_Revenue ?revenue .\n"+
"	?customer skos:broader ?c_city .\n"+
"	?c_city qb4o:inLevel ssb:City .\n"+
"	?c_city skos:broader ?c_nation .\n"+
"	?c_nation qb4o:inLevel ssb:Nation .\n"+
"	?c_nation ssb:Nation_Name  'UNITEDSTATES' .\n"+
"	?c_city ssb:City_Name ?c_cityname .\n"+
"	?supplier skos:broader ?s_city .\n"+
"	?s_city qb4o:inLevel ssb:City .\n"+
"	?s_city skos:broader ?s_nation .\n"+	
"	?s_nation ssb:Nation_Name ?s_nn . \n"+
"	?s_nation qb4o:inLevel ssb:Nation .\n"+
"	?s_city ssb:City_Name ?s_cityname . \n"+
"	?lo_orderdate skos:broader ?month .\n"+
"	?month qb4o:inLevel ssb:Month .\n"+
"	?month skos:broader ?year .\n"+
"	?year qb4o:inLevel ssb:Year .\n"+
"	?year ssb:Date_Year ?d_year ."+
"	FILTER(?d_year >= 1992 && ?d_year <= 1997) \n"+
"	FILTER(?s_nn = 'UNITEDSTATES') \n"+
"}\n"+
"GROUP BY ?c_city ?s_city ?d_year \n"+
"ORDER BY ASC(?d_year) DESC(?lo_revenue)";
			
			String matView = "prefix xsd: <http://www.w3.org/2001/XMLSchema#> \n"+
					"prefix qb: <http://purl.org/linked-data/cube#>\n"+ 
					"prefix skos: <http://www.w3.org/2004/02/skos/core#>\n"+ 
					"prefix qb4o: <http://purl.org/qb4olap/cubes#> \n"+
					"prefix ssb: <http://ssb.org/ssb#> \n"+
					"prefix ssb-inst: <http://ssb.org/ssb-inst#>\n"+
					"CONSTRUCT { ?mv_id a qb:Observation ; \n"+
					"qb:dataSet ssb-inst:dataset-aggview_2 ; \n"+
					"ssb:Part_Manufacturer ?mv_mfgr ; \n"+
					"ssb:OrderDate_Year ?mv_year ; \n"+
					"ssb:Customer_City ?mv_c_city ; \n"+
					"ssb:Supplier_City ?mv_s_city ; \n"+
					"ssb:RevenueCount ?c_revenue ; \n"+
					"ssb:PossibleRevenue ?poss_revenue ; \n"+
					"ssb:Revenue ?sum_rev ; \n"+
					"ssb:Profit ?sum_profit . } \n"+
					"WHERE { \n"+
					"SELECT ?mv_id ?mv_mfgr ?mv_c_city ?mv_s_city ?mv_year (SUM(?mv_revenue) AS ?sum_rev) \n"+
  "  (COUNT(?mv_revenue) AS ?c_revenue) (SUM(?mv_revenue - ?supplycost) AS ?sum_profit) \n"+
  "  (SUM(?mv_extendedprice * ?mv_discount) AS ?poss_revenue) \n"+
 " WHERE { \n"+
  "  ?li a qb:Observation ; \n"+
  "   qb:dataSet ssb-inst:dataset1 ; \n"+
  "   ssb:Lineorder_Part ?mv_part ; \n"+
  "    ssb:Lineorder_OrderDate ?mv_lo_orderdate ; \n"+
  "    ssb:Lineorder_Customer ?mv_customer ; \n"+
  "    ssb:Lineorder_Supplier ?mv_supplier  ; \n"+
  "    ssb:Lineorder_Revenue ?mv_revenue ; \n"+
  "    ssb:Lineorder_SupplyCost ?mv_supplycost ; \n"+
  "    ssb:Lineorder_ExtendedPrice ?mv_extendedprice ; \n"+
  "    ssb:Lineorder_Discount ?mv_discount . \n"+
  "    ?mv_customer skos:broader ?mv_c_city . \n"+
  "    ?mv_c_city qb4o:inLevel ssb:City . \n"+
  "    ?mv_supplier skos:broader ?mv_s_city . \n"+
  "    ?mv_s_city qb4o:inLevel ssb:City . \n"+
  "    ?mv_part skos:broader ?mv_brand . \n"+
  "    ?mv_part qb4o:inLevel ssb:Brand . \n"+
  "    ?mv_brand skos:broader ?mv_category . \n"+
  "    ?mv_category qb4o:inLevel ssb:Category . \n"+
  "    ?mv_category skos:broader ?mv_mfgr . \n"+
  "    ?mv_mfgr qb4o:inLevel ssb:Manufacturer . \n"+
  "    ?mv_lo_orderdate skos:broader ?mv_month . \n"+
  "    ?mv_month qb4o:inLevel ssb:Month . \n"+
  "    ?mv_month skos:broader ?mv_year . \n"+
  "    ?mv_year qb4o:inLevel ssb:Year . \n"+
  "    BIND(IRI(CONCAT('http://ssb.org/ssb-inst#', STRAFTER(str(?mv_c_city), 'http://ssb.org/ssb-inst#'), \n"+ 
  "    STRAFTER(str(?mv_s_city), 'http://ssb.org/ssb-inst#'), STRAFTER(str(?mv_year), 'http://ssb.org/ssb-inst#'), \n"+ 
  "    STRAFTER(str(?mv_mfgr), 'http://ssb.org/ssb-inst#'))) AS ?mv_id) . \n"+
  "} \n"+
  "GROUP BY ?mv_id ?mv_mfgr ?mv_c_city ?mv_s_city ?mv_year \n"+
  "}";
			
			
			
			
			SPARQLParser sprqlQ = new SPARQLParser();
			ParsedQuery pquer = sprqlQ.parseQuery(query, "http://10.15.120.1:8890/sparql");
			TupleExpr te = pquer.getTupleExpr();
			List<StatementPattern> queryStats = GetStatementPatterns(te);
			
			ParsedQuery pquerMV = sprqlQ.parseQuery(matView, "http://10.15.120.1:8890/sparql");
			TupleExpr teMV = pquerMV.getTupleExpr();
			List<StatementPattern> mvStats = GetStatementPatterns(teMV);
			
			//List<Compare> ff = GetFilters(te);
			List<String> filterVars = GetFilterVars(te);
			List<String> grpVars = GetGroupingVars(te);
			List<String> dimGrpVars = new ArrayList<String>();
			for (String grpVar : grpVars) {
				dimGrpVars.add(GetDimensionFromGroupingVar(grpVar, queryStats));
			}
			List<String> queryVarStr = GetQueryMeasureVars(te);
			List<String> mvVarStr = GetQueryMeasureVars(teMV);
			
			List<String> dimensionVars = new ArrayList<String>();
			dimensionVars.addAll(dimGrpVars);
			//// find what comes before - filter or grouping vars
			for (String fltr : filterVars) {
				String filterDimVar = GetDimensionFromFilterVar(fltr, queryStats);
				for (String grp : dimGrpVars) {
					String lower = GetLowerHierarchyVar(filterDimVar, grp, GetDimensionStatementPatterns(queryStats));
					if(lower != null){
						System.out.println(lower);
						dimensionVars.remove(grp);
						dimensionVars.add(lower);
						break;
					}
				}
			}
			
			System.out.println(dimensionVars);//// all needed dimension vars.
			
			String queryRootNode = GetRootVar(te);
			String mvRootNode = GetRootVar(teMV);
			
			List<String> queryDimensionPredicates = new ArrayList<String>();
			for (String dim : dimensionVars) {
				queryDimensionPredicates.add(GetFirstConnectingPredicate(dim, queryRootNode, queryStats));
			}
			
			//List<String> queryRootPredicates = GetConnectedPredicates(queryRootNode, queryStats);
			List<String> matviewRootPredicates = GetConnectedPredicates(mvRootNode, mvStats);
			
			if(!IsContainsAllDimensions(queryDimensionPredicates, matviewRootPredicates))
				return;///// CANNOT DO IT
			
			List<String> queryMeasurePredicates = new ArrayList<String>();
			for (String mes : queryVarStr) {
				queryMeasurePredicates.add(GetFirstConnectingPredicate(mes, queryRootNode, queryStats));
			}
			List<String> matViewMeasurePredicates = new ArrayList<String>();
			for (String mes : mvVarStr) {
				matViewMeasurePredicates.add(GetFirstConnectingPredicate(mes, mvRootNode, mvStats));
			}
			
			if(!IsContainsAllMeasures(queryMeasurePredicates, matViewMeasurePredicates))
				return;///// CANNOT DO IT
			
			List<StatementPattern> queryRootSPs = GetDirectlyConnectedStatementPatterns(queryRootNode, queryStats); 
			List<StatementPattern> mvRootSPs = GetDirectlyConnectedStatementPatterns(mvRootNode, mvStats);
			for (String queryDimPredicate : queryDimensionPredicates) {
				List<StatementPattern> queryDimStats = null;
				for (StatementPattern sp : queryRootSPs) {
					if(sp.getPredicateVar().getValue().stringValue().equals(queryDimPredicate)){
						String firstDimVar = sp.getObjectVar().getName();
						queryDimStats = GetDimensionStatementPatterns(firstDimVar, dimensionVars, queryStats);
					}
				}
				List<StatementPattern> mvDimStats = null;
				for (StatementPattern sp : mvRootSPs) {
					if(sp.getPredicateVar().getValue().stringValue().equals(queryDimPredicate)){
						String firstDimVar = sp.getObjectVar().getName();
						mvDimStats = GetDimensionStatementPatterns(firstDimVar, mvStats);
					}
				}
				//// now compare
				if(!IsProperLevel(queryDimStats, mvDimStats))
					return; ///// CANNOT DO IT
			}
			
			System.out.println("Can be used...");
			
			System.out.println(te.toString());
			
			List<String> mvSelVars = new ArrayList<String>();
			List<String> rewrittenMeasures = null;
			List<StatementPattern> keepPtrns = null;
			List<StatementPattern> mvPtrns = null;
			List<List<StatementPattern>> keepNdeletePtrns = DivideQueryBasedOnMV(queryRootNode, queryStats, mvRootNode, mvStats, dimensionVars);
			mvPtrns = keepNdeletePtrns.get(1);
			keepPtrns = keepNdeletePtrns.get(0);
			
			if(teMV instanceof UnaryTupleOperator){
				TupleExpr uto = (UnaryTupleOperator)teMV;
				while (uto instanceof UnaryTupleOperator){
					if(uto instanceof MultiProjection){
						MultiProjection mp = (MultiProjection)uto;
						List<ProjectionElemList> prjctns = mp.getProjections();
						Extension ext = (Extension)mp.getArg();
						//List<ExtensionElem> extElems = ext.getElements(); //// values for predicates and objects
						//GetMVSelectMeasureElems(teMV);
						Object[] retObj = GetRewrittenMeasures(queryRootNode, queryStats, queryVarStr, mvRootNode, mvStats,
								GetMeasureElems(te), GetMVSelectMeasureElems(teMV), GetViewConstructMeasureElems(teMV), prjctns);
						rewrittenMeasures = (List<String>)retObj[0];
						List<StatementPattern> deletePtrns = (List<StatementPattern>)retObj[1];
						for (StatementPattern sp : deletePtrns) {
							keepPtrns.remove(sp);
						}
						List<StatementPattern> addPtrns = (List<StatementPattern>)retObj[2];
						for (StatementPattern sp : addPtrns) {
							if(!mvPtrns.contains(sp))
								mvPtrns.add(sp);
						}
						Map<String, String> aggFunVars = (Map<String, String>)retObj[3];
						for (String mvAggStr : aggFunVars.values()) {
							if(!mvSelVars.contains(mvAggStr))
								mvSelVars.add(mvAggStr);
						}
					}
					uto = ((UnaryTupleOperator)uto).getArg();
				}
			}
			
			Object[] objMaps = GetCounterpartVarNamesQueryAndView(queryRootNode, queryStats, mvRootNode, mvStats);
			Map<String, String> qryToViewVars = (Map<String, String>)objMaps[0];
						
			ParsedQuery pq = new ParsedTupleQuery(te);
			
			List<String> selectionVars = GetSimpleSelectVars(te);
			
			
			for (String selVar : dimensionVars) {
				if(qryToViewVars.get(selVar) != null && !mvSelVars.contains(qryToViewVars.get(selVar)))
					mvSelVars.add(qryToViewVars.get(selVar));
			}
//			for (String msrVar : queryVarStr) {
//				if(qryToViewVars.get(msrVar) != null && !mvSelVars.contains(qryToViewVars.get(msrVar)))
//					mvSelVars.add(qryToViewVars.get(msrVar));
//			}
			
			List<String> mvtuples = GetMVConstructElements(teMV, mvSelVars);
			
			List<String> filters = new ArrayList<String>();
			Filter f = GetFilter(te);
			if(f != null)
				filters = GetFilterStrings(f);
			
			String rewQuery = ReplaceQuery(query, mvtuples, keepPtrns, "http://matview5.ssb.org", selectionVars, rewrittenMeasures, filters);//mvPtrns,
			
			System.out.println(rewQuery);

//			TupleQuery tplQr = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//			System.out.println(tplQr.toString());
//			BindingSet setquery = tplQr.getBindings();
//			for (Binding bndng : setquery) {
//				System.out.println("query binding name " + bndng.getName());
//			}
//			TupleQueryResult results = tplQr.evaluate();
//			List<String> bindingNames = results.getBindingNames();
//			
//			while(results.hasNext()){
//				BindingSet set = results.next();
//				for (String varName : bindingNames) {
//					Value val = set.getValue(varName);
//					System.out.println(varName+" "+val.stringValue());
//				}
//			}
//			results.close();
			
			conn.close();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error: "+e.getMessage());
		} //catch (QueryEvaluationException e) {
//			// TODO Auto-generated catch block
//			System.out.println("Error: "+e.getMessage());
//			e.printStackTrace();
//		}
	}
	
	public static void testQuery(){
		long startTime = System.nanoTime();
		List<MatView> mvs = MatViewsLoader.LoadMatViews();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime);
		Logger.log(String.format("MatView Load Duration %s, %s ms", duration, ((double)duration/1000000)));
		List<QB4OQuery> qries = MatViewsLoader.LoadQueriess();
		for (QB4OQuery qb4oQuery : qries) {
			startTime = System.nanoTime();
			Object[] obj = HandleQuery(qb4oQuery, mvs);
			endTime = System.nanoTime();
			duration = (endTime - startTime);
			Logger.log(String.format("Query %s (%s) rewriting duration %s, %s ms", qb4oQuery.getName(), qb4oQuery.getNumber(), duration, ((double)duration/1000000)));
			
			if(obj != null){
				String query = (String)obj[0];
				MatView selectedView = (MatView)obj[1];
				
//				try {
//					//// now execute the rewritten query
//					Repository rep = new SPARQLRepository("http://10.15.120.1:8890/sparql");
//					rep.initialize();
//					RepositoryConnection conn = rep.getConnection();
//					TupleQuery tplQr = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//					Query Qr = conn.prepareQuery(QueryLanguage.SPARQL, query);
//					
//					TupleQueryResult results = tplQr.evaluate();
//					int i = 0;//// number of results
//					while(results.hasNext()){
//						BindingSet set = results.next();
//						i++;
//					}
//					results.close();
//					conn.close();
//					endTime = System.nanoTime();
//					duration = (endTime - startTime);
//					Logger.logExecution(qb4oQuery, selectedView, query, ((double)duration/1000000), i);
//					Logger.log(String.format("Query %s (%s) execution duration %s, %s ms, result size %s", qb4oQuery.getName(), qb4oQuery.getNumber(), duration, ((double)duration/1000000), i));
//				} catch (RepositoryException e) {
//					Logger.log(e.getMessage());
//					e.printStackTrace();
//				} catch (MalformedQueryException e) {
//					Logger.log(e.getMessage());
//					e.printStackTrace();
//				} catch (QueryEvaluationException e) {
//					Logger.log(e.getMessage());
//					e.printStackTrace();
//				}	
			}
		}
		
//		for (QB4OQuery qb4oQuery : qries) {
//			startTime = System.nanoTime();
//			Object[] obj = HandleQuery(qb4oQuery, mvs);
//			endTime = System.nanoTime();
//			duration = (endTime - startTime);
//			Logger.log(String.format("Query %s (%s) rewriting duration %s, %s ms", qb4oQuery.getName(), qb4oQuery.getNumber(), duration, ((double)duration/1000000)));
//			
//			if(obj != null){
//				try {
//				//// now execute the normal query
//					startTime = System.nanoTime();
//					Repository rep = new SPARQLRepository("http://10.15.120.1:8890/sparql");
//					rep.initialize();
//					RepositoryConnection conn = rep.getConnection();
//					
//					TupleQuery tplQrNormal = conn.prepareTupleQuery(QueryLanguage.SPARQL, qb4oQuery.getQuery()); 
//					TupleQueryResult resultsNormal = tplQrNormal.evaluate();
//					int i = 0;//// number of results
//					while(resultsNormal.hasNext()){
//						BindingSet set = resultsNormal.next();
//						i++;
//					}
//					resultsNormal.close();
//					conn.close();
//					endTime = System.nanoTime();
//					duration = (endTime - startTime);
//					Logger.logExecution(qb4oQuery, ((double)duration/1000000), i);
//					Logger.log(String.format("Query %s (%s) execution duration %s, %s ms, result size %s", qb4oQuery.getName(), qb4oQuery.getNumber(), duration, ((double)duration/1000000), i));
//				} catch (RepositoryException e) {
//					Logger.log(e.getMessage());
//					e.printStackTrace();
//				} catch (MalformedQueryException e) {
//					Logger.log(e.getMessage());
//					e.printStackTrace();
//				} catch (QueryEvaluationException e) {
//					Logger.log(e.getMessage());
//					e.printStackTrace();
//				}
//			}
//		}
	}
	
	private static Object[] HandleQuery(QB4OQuery query, List<MatView> views){
		//Repository rep = new SPARQLRepository("http://10.15.120.1:8890/sparql");
		List<MatView> candidateViews = new ArrayList<MatView>();
		MatView selectedMatView = null;
		try {
			SPARQLParser sprqlQ = new SPARQLParser();
			ParsedQuery pquer = sprqlQ.parseQuery(query.getQuery(), "http://10.15.120.1:8890/sparql");
			TupleExpr teQuery = pquer.getTupleExpr();
			
			String queryRootNode = GetRootVarUpdated(teQuery);
			
			List<DimensionInfo> dimensions = getQueryDimensions(teQuery);
			List<AggregateInfo> aggregates = getQueryMeasures(teQuery);
			
			List<StatementPattern> queryStats = GetStatementPatterns(teQuery);
			List<StatementPattern> queryRootSPs = new ArrayList<StatementPattern>();
			List<String> filterVars = new ArrayList<String>(); //GetFilterVars(te); //// query filter variables
			//List<String> grpVars = GetGroupingVars(te); //// query groupby variables
			List<String> dimGrpVars = new ArrayList<String>(); //// hierarchical variables from groupby (group by may include vars that are not directly hierarchical but connected to H)
			List<String> dimensionVars = new ArrayList<String>(); //// selected hierarchical vars - will be formed from hierarchical or filter variables
			List<String> queryDimensionPredicates = new ArrayList<String>(); //// predicates connecting root to dimensions
			for (DimensionInfo dimension : dimensions) {
				if(dimension.getFilterVar() != null)
					filterVars.add(dimension.getFilterVar());
				if(dimension.getGroupByVar() != null)
					dimGrpVars.add(dimension.getGroupByVar());
				if(dimension.getSelectedDimVar() != null)
					dimensionVars.add(dimension.getSelectedDimVar());
				if(dimension.getPredicate() != null)
					queryDimensionPredicates.add(dimension.getPredicate());
				
				queryRootSPs.addAll(dimension.getPatterns());
			}
			
			List<String> queryMsrVarStr = new ArrayList<String>();//variables that will be aggregated
			List<String> queryMeasurePredicates = new ArrayList<String>(); //// predicates connecting root to measures
			for (AggregateInfo aggInfo : aggregates) {
				queryMsrVarStr.addAll(aggInfo.getVariables());
				queryMeasurePredicates.addAll(aggInfo.getPredicates());
				queryRootSPs.addAll(aggInfo.getPatterns());
			}
			queryMsrVarStr = new ArrayList<String>(new HashSet<String>(queryMsrVarStr));
			queryMeasurePredicates = new ArrayList<String>(new HashSet<String>(queryMeasurePredicates));
			queryRootSPs = new ArrayList<StatementPattern>(new HashSet<StatementPattern>(queryRootSPs));
			
			//// now check matviews
			String mvRootNode = null;
			List<StatementPattern> mvStats = null;
			for (MatView matView : views) {
				TupleExpr teMV = matView.getTupleExpr();
				teMV = GetSelectExpressionFromView(teMV);
				
				List<DimensionInfo> mvDimensions = getQueryDimensions(teMV);
				List<String> mvDimensionPredicates = new ArrayList<String>();
				for (DimensionInfo mvDimensionInfo : mvDimensions) {
					mvDimensionPredicates.add(mvDimensionInfo.getPredicate());
				}
				
				if(!IsContainsAllDimensions(queryDimensionPredicates, mvDimensionPredicates))
					continue;///// CANNOT USE CURRENT VIEW
				
				List<AggregateInfo> viewAggregates = getQueryMeasures(teMV);
				boolean hasAllAggregates = true;
				for (AggregateInfo aggInfo : aggregates) {
					if(!IsContainsAggregateInfo(aggInfo, viewAggregates))
						hasAllAggregates = false;///// CANNOT USE CURRENT VIEW
				}
				if(!hasAllAggregates)
					continue;
				
				boolean canUseCurrectView = true;
				for (String queryDimPredicate : queryDimensionPredicates) {
					DimensionInfo queryDI = null;
					DimensionInfo viewDI = null;
					for (DimensionInfo di : dimensions) {
						if(di.getPredicate().equals(queryDimPredicate)){
							queryDI = di;
							break;
						}
					}
					for (DimensionInfo di : mvDimensions) {
						if(di.getPredicate().equals(queryDimPredicate)){
							viewDI = di;
							break;
						}
					}
					
					//// now compare
					if(!IsProperLevel(queryDI, viewDI)){
						canUseCurrectView = false;///// CANNOT USE CURRENT VIEW
						break;
					}
				}
				if(canUseCurrectView) {//// current view can be used
					System.out.println(String.format("Current view name: %s, number: %s, cost %s can be used...", matView.getName(), matView.getNumber(), matView.getCost()));
					//selectedMatView = matView;
					candidateViews.add(matView);
					//mvStats = GetStatementPatterns(teMV);
					//mvRootNode = GetRootVarUpdated(teMV);
				}
			}
			//// choose view with min cost
			if(candidateViews.size() > 0){
				int minCost = 0;
				for (MatView view : candidateViews) {
					if(minCost == 0) {
						minCost = view.getCost();
						selectedMatView = view;
					}
					if(view.getCost() < minCost){
						minCost = view.getCost();
						selectedMatView = view;
					}
				}
			}
			
			////REWRITING - Gives Errors - need to check
			if(selectedMatView != null){
				System.out.println("Query " + query.getName() + ", selected view " + selectedMatView.getName());

				TupleExpr viewTE = GetSelectExpressionFromView(selectedMatView.getTupleExpr());
				mvStats = GetStatementPatterns(viewTE);
				mvRootNode = GetRootVarUpdated(viewTE);
				List<AggregateInfo> viewAggregates = getQueryMeasures(viewTE);
				List<DimensionInfo> viewDimensions = getQueryDimensions(viewTE);
				Map<AggregateInfo, AggregateInfo> viewToQryAggInfo = getSameAggregates(aggregates, viewAggregates);
				
				List<String> mvSelVars = new ArrayList<String>();
				//List<String> rewrittenMeasures = null;
				List<StatementPattern> keepPtrns = null;
				List<StatementPattern> mvPtrns = null;
				List<List<StatementPattern>> keepNdeletePtrns = separateQueryTriplesBasedOnView(queryRootNode, queryStats, mvRootNode, mvStats, dimensions);//DivideQueryBasedOnMV(queryRootNode, queryStats, mvRootNode, mvStats, dimensionVars);
				mvPtrns = keepNdeletePtrns.get(1);
				keepPtrns = keepNdeletePtrns.get(0);
				
				List<ExtensionElem> viewConstructEEList = GetViewConstructMeasureElems(selectedMatView.getTupleExpr());
				
				//// delete all triples related to measures from kept triples
				for (AggregateInfo aggInfo : aggregates) {
					List<StatementPattern> aggPtrns = aggInfo.getPatterns();
					keepPtrns.removeAll(aggPtrns);
				}
				//// create new measures as strings
				List<ProjectionElemList> prjctns = GetViewConstructProjectionElems(selectedMatView.getTupleExpr());
				String rewrittenSelectString = getRewrittenSelectVars(teQuery, prjctns, viewConstructEEList, viewToQryAggInfo);
				
				Map<String, String> viewToQryDimVarMap = getViewToQueryDimensionMapping(dimensions, viewDimensions);
				List<String> namedGraphDimensionTriples = getRewrittenGraphTriples(selectedMatView.getTupleExpr(), viewToQryDimVarMap);
				
				List<String> msrTriples = getRewrittenGraphTriplesForAggregates(selectedMatView.getTupleExpr(), new ArrayList<AggregateInfo>(viewToQryAggInfo.keySet()));
				namedGraphDimensionTriples.addAll(msrTriples);
				
				List<String> filters = new ArrayList<String>();
				Filter f = GetFilter(teQuery);
				if(f != null)
					filters = GetFilterStrings(f);
				
				String rewQuery = rewriteQuery(query.getQuery(), namedGraphDimensionTriples, keepPtrns, selectedMatView.getGraph(), rewrittenSelectString, filters);
				
				System.out.println(rewQuery);
				
				Object[] obj = new Object[2];
				obj[0] = rewQuery;
				obj[1] = selectedMatView;
				return obj;
			}

		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error: "+e.getMessage());
		}
		return null;
	}
	
	private static List<StatementPattern> GetStatementPatterns(TupleExpr expr){
		ArrayList<StatementPattern> statements = new ArrayList<StatementPattern>();
		if(expr instanceof UnaryTupleOperator){
			TupleExpr te = ((UnaryTupleOperator) expr).getArg();
			return GetStatementPatterns(te);
		}
		else if(expr instanceof BinaryTupleOperator){
			BinaryTupleOperator bto = (BinaryTupleOperator)expr;
			TupleExpr leftExpr = bto.getLeftArg();
			TupleExpr rightExpr = bto.getRightArg();
			if(rightExpr instanceof StatementPattern){
				statements.add((StatementPattern)rightExpr);
			}
			else if(rightExpr instanceof BinaryTupleOperator){
				statements.addAll(GetStatementPatterns(rightExpr));
			}
			
			if(leftExpr instanceof StatementPattern){
				statements.add((StatementPattern)leftExpr);
			}
			else if(leftExpr instanceof BinaryTupleOperator){
				statements.addAll(GetStatementPatterns(leftExpr));
			}
		}
		return statements;
	}
	
	///min distance between these variables.
	private static int GetMinDistant(String varStart, String varEnd, List<StatementPattern> stats){
		int distant = 0;
		List<StatementPattern> lst = new ArrayList<StatementPattern>();
		for (StatementPattern statementPattern : stats) {
			if(GetVarNames(statementPattern.getVarList()).contains(varStart)){
				lst.add(statementPattern);//// all the patterns where varStart exists
			}
		}
		////\\\\\ if the patterns 
		for (StatementPattern statementPattern : lst) {
			if(GetVarNames(statementPattern.getVarList()).contains(varEnd)){
				distant = 1;
				return distant;
			}
		}
		//// it is not directly connected
		//// remove them from all patterns because we will search within the rest
		//// need another arrayList to remove statement patterns from that list
		List<StatementPattern> reducedStats = new ArrayList<StatementPattern>();
		for (StatementPattern statementPattern : stats) {
			reducedStats.add(statementPattern);
		}
		boolean removed = reducedStats.removeAll(lst);
		int minCost = 0;
		if(removed){
			for (StatementPattern statementPattern : lst) {
				List<String> statementVars = GetVarNames(statementPattern.getVarList());
				if(statementVars.size() == 2){
					//// the other var in statement pattern
					statementVars.remove(varStart);
					String theOtherVar =  statementVars.get(0);
					int cost = GetMinDistant(theOtherVar, varEnd, reducedStats);
					if(cost > 0){
						if(minCost == 0) minCost = cost+1;
						if((cost+1) < minCost)
							minCost = cost+1;
					}
				}
			}
			if(minCost > 0) distant = minCost;
		}
		return distant;
	}
	
	/// helper method
	private static List<String> GetVarNames(List<Var> varLst){
		List<String> varList = new ArrayList<String>();
		for (Var vr : varLst) {
			if(!vr.isConstant())
				varList.add(vr.getName());
		}
		return varList;
	}
	
	//// all vairables of the query
	private static List<String> GetAllVars(List<StatementPattern> stats){
		Set<String> vars = new HashSet<String>();
		for (StatementPattern statementPattern : stats) {
			if(!statementPattern.getSubjectVar().isConstant()) {
				String varName = statementPattern.getSubjectVar().getName();
				vars.add(varName);
			}
			if(!statementPattern.getObjectVar().isConstant()) {
				String varName = statementPattern.getObjectVar().getName();
				vars.add(varName);
			}
		}
		return new ArrayList<String>(vars);
	}

	private static Filter GetFilter(TupleExpr tuple){
		if(tuple instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)tuple;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Filter){
					return ((Filter)uto);					
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		return null;
	}

	private static List<String> GetFilterVars(TupleExpr tuple){
		List<Compare> filters = new ArrayList<Compare>();
		if(tuple instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)tuple;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Filter){
					ValueExpr ve = ((Filter)uto).getCondition();
					if(ve instanceof Compare){
						Compare nd = (Compare)ve;
						filters.add(nd);
					}
					else if(ve instanceof And){
						And nd = (And)ve;
						ValueExpr veleft = nd.getLeftArg();
						ValueExpr veright = nd.getRightArg();
						if(veleft instanceof Compare)
							filters.add((Compare)veleft);
						if(veright instanceof Compare)
							filters.add((Compare)veright);						
					}
					else if(ve instanceof Or){
						Or nd = (Or)ve;
						ValueExpr veleft = nd.getLeftArg();
						ValueExpr veright = nd.getRightArg();
						if(veleft instanceof Compare)
							filters.add((Compare)veleft);
						if(veright instanceof Compare)
							filters.add((Compare)veright);
					}
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		List<String> filterVars = new ArrayList<String>();
		for (Compare compare : filters) {
			Var vel = (Var)compare.getLeftArg();
			if(!filterVars.contains(vel.getName()))
				filterVars.add(vel.getName());
			//ValueExpr ver = compare.getRightArg();
		}
		return filterVars;
	}
	
	private static List<String> GetGroupingVars(TupleExpr te){
		List<String> varLst = new ArrayList<String>();
		if(te instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)te;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Group){
					Set<String> vrsSet =((Group)uto).getGroupBindingNames();
					varLst.addAll(vrsSet);
					break;
					//vrsSet.toArray();
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
			
		}
		return varLst;
	}
	
	private static List<String> GetSimpleSelectVars(TupleExpr te){
		List<String> varLst = new ArrayList<String>();
		if(te instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)te;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Projection){
					ProjectionElemList prjctsSet =((Projection)uto).getProjectionElemList();
					for (ProjectionElem pe : prjctsSet.getElements()) {
						if(!pe.hasAggregateOperatorInExpression())
							varLst.add(pe.getTargetName());
					}
					
					break;
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
			
		}
		return varLst;
	}
	
	private static String getRewrittenSelectVars(TupleExpr queryTE, List<ProjectionElemList> constructPrjctns, List<ExtensionElem> viewConstructEEList, Map<AggregateInfo, AggregateInfo> viewToQryAggInfo){
		StringBuilder varLst = new StringBuilder();
		if(queryTE instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)queryTE;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Projection){
					ProjectionElemList prjctsSet =((Projection)uto).getProjectionElemList();
					for (ProjectionElem peSelect : prjctsSet.getElements()) {
						if(!peSelect.hasAggregateOperatorInExpression())
							varLst.append("?" + peSelect.getTargetName() + " ");
						else{
							for (AggregateInfo viewAI : viewToQryAggInfo.keySet()) {
								ExtensionElem eeMV = viewAI.getFunction();
								for (ProjectionElemList peList : constructPrjctns) {
									for (ProjectionElem pe : peList.getElements()) {
										if(pe.getTargetName().equals("object") && pe.getSourceName().equals(eeMV.getName())){
//											for (ExtensionElem eeConstr : viewConstructEEList) {
//												if(eeConstr.getName().equals(peList.getElements().get(1).getSourceName())){
//													//String prdct = ((ValueConstant)eeConstr.getExpr()).getValue().stringValue();
//													//System.out.println("predicate is " + prdct); //peList.getElements().get(1).getSourceName());
//													//System.out.println("Triple:  ?" + mvRootNode + " <" + prdct + "> ?" + eeMV.getName() + " ."); //peList.getElements().get(1).getSourceName());
//												}
//											}
											if(eeMV.getExpr() instanceof Sum){
												varLst.append("(SUM(?"+ eeMV.getName() + ") AS ?" + viewToQryAggInfo.get(viewAI).getFunction().getName() + ") ");
											}
											else if(eeMV.getExpr() instanceof Count){
												varLst.append("(COUNT(?"+ eeMV.getName() + ") AS ?" + viewToQryAggInfo.get(viewAI).getFunction() + ") ");
											}
											else if(eeMV.getExpr() instanceof Min){
												varLst.append("(MIN(?"+ eeMV.getName() + ") AS ?" + viewToQryAggInfo.get(viewAI).getFunction() + ") ");
											}
											else if(eeMV.getExpr() instanceof Max){
												varLst.append("(MAX(?"+ eeMV.getName() + ") AS ?" + viewToQryAggInfo.get(viewAI).getFunction() + ") ");
											}
										}
									}
								}
							}
						}
					}
					
					break;
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
			
		}
		return varLst.toString();
	}

	private static List<String> GetQueryMeasureVars(TupleExpr te){
		List<String> vrStr = new ArrayList<String>() ;
		if(te instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)te;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Extension){
					for(ExtensionElem elem : ((Extension)uto).getElements()){
						ValueExpr ve = elem.getExpr();
						if(ve instanceof Sum){
							ValueExpr innerVE = ((Sum)ve).getArg();
							if(innerVE instanceof Var)
								vrStr.add(((Var)innerVE).getName());
							else if(innerVE instanceof MathExpr){
								vrStr.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
								vrStr.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
							}
						}
						else if(ve instanceof Avg){
							ValueExpr innerVE = ((Avg)ve).getArg();
							if(innerVE instanceof Var)
								vrStr.add(((Var)innerVE).getName());
							else if(innerVE instanceof MathExpr){
								vrStr.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
								vrStr.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
							}
						}
						else if(ve instanceof Count){
							ValueExpr innerVE = ((Count)ve).getArg();
							if(innerVE instanceof Var)
								vrStr.add(((Var)innerVE).getName());
							else if(innerVE instanceof MathExpr){
								vrStr.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
								vrStr.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
							}
						}
						else if(ve instanceof Min){
							ValueExpr innerVE = ((Min)ve).getArg();
							if(innerVE instanceof Var)
								vrStr.add(((Var)innerVE).getName());
							else if(innerVE instanceof MathExpr){
								vrStr.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
								vrStr.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
							}
						}
						else if(ve instanceof Max){
							ValueExpr innerVE = ((Max)ve).getArg();
							if(innerVE instanceof Var)
								vrStr.add(((Var)innerVE).getName());
							else if(innerVE instanceof MathExpr){
								vrStr.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
								vrStr.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
							}
						}
					}
					break;//// no need to search further - no need to go into joins
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}			
		}
		return vrStr;
	}

	private static List<ExtensionElem> GetMeasureElems(TupleExpr te){
		if(te instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)te;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Extension){
					return ((Extension)uto).getElements();
					
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}			
		}
		return null;
	}
	
	private static List<ExtensionElem> GetViewConstructMeasureElems(TupleExpr teMV){
		if(teMV instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)teMV;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof MultiProjection){
					MultiProjection mp = (MultiProjection)uto;
					//List<ProjectionElemList> prjctns = mp.getProjections();
					Extension ext = (Extension)mp.getArg();
					return ext.getElements(); 					
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		return null;
	}
	
	private static List<ProjectionElemList> GetViewConstructProjectionElems(TupleExpr teMV){
		if(teMV instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)teMV;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof MultiProjection){
					MultiProjection mp = (MultiProjection)uto;
					List<ProjectionElemList> prjctns = mp.getProjections();
					return prjctns; 					
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		return null;
	}
	
	private static List<ExtensionElem> GetMVSelectMeasureElems(TupleExpr te){
		if(te instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)te;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Projection){
					
					while (uto instanceof UnaryTupleOperator){
						if(uto instanceof Extension){
							return ((Extension)uto).getElements();
						}
						uto = ((UnaryTupleOperator)uto).getArg();
					}
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}			
		}
		return null;
	}
	
	private static String GetLowerHierarchyVar(String var1, String var2, List<StatementPattern> dimStats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		//String retStr = null;
		String varStart = var1;
		String varEnd = var2;
		if(var1.equals(var2)) return var1;
		//// find if var1 is before var2
		for (StatementPattern statementPattern : dimStats) {
			for (StatementPattern sp : dimStats) {
				if(sp.getSubjectVar().getName().equals(varStart) && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& !sp.getObjectVar().isConstant()){
					if(sp.getObjectVar().getName().equals(varEnd))
						return var1;
					else{
						varStart = sp.getObjectVar().getName();
						break;
					}
				}
			}
			
		}
		
		//// find if var2 is before var1
		varStart = var2;
		varEnd = var1;
		for (StatementPattern statementPattern : dimStats) {
			for (StatementPattern sp : dimStats) {
				if(sp.getSubjectVar().getName().equals(varStart) && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& !sp.getObjectVar().isConstant()){
					if(sp.getObjectVar().getName().equals(varEnd))
						return var2;
					else{
						varStart = sp.getObjectVar().getName();
						break;
					}
				}
			}

		}
		
		return null;
	}
	
	private static List<StatementPattern> GetDimensionStatementPatterns(List<StatementPattern> stats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		List<StatementPattern> dims = new ArrayList<StatementPattern>();
		for (StatementPattern sp : stats) {
			if(sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o)){
				dims.add(sp);
			}
		}
		return dims;
	}
	
	private static String GetDimensionFromFilterVar(String filterVar, List<StatementPattern> stats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		String prefix_qb = "http://www.w3.org/2004/02/skos/core#narrower";
		List<String> possibleVars = new ArrayList<String>();
		for (StatementPattern sp : stats) {
			if(!sp.getSubjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
					&& (sp.getObjectVar().getName().equals(filterVar) || sp.getSubjectVar().getName().equals(filterVar))){
				return filterVar;
			}
			else if(!sp.getSubjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) 
					&& (sp.getObjectVar().getName().equals(filterVar) || sp.getSubjectVar().getName().equals(filterVar))){
				return filterVar;
			}
		}
		
		for (StatementPattern sp : stats) {
			if(!sp.getSubjectVar().isConstant() && sp.getObjectVar().getName().equals(filterVar)){
				possibleVars.add(sp.getSubjectVar().getName());
			}
		}
		for (String pv : possibleVars) {
			for (StatementPattern sp : stats) {
//				if(!sp.getSubjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
//						&& sp.getObjectVar().getName().equals(pv)){
//					return sp.getObjectVar().getName();
//				}
				if(!sp.getSubjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& sp.getObjectVar().getName().equals(pv)){
					return sp.getObjectVar().getName();
				}
				else if(!sp.getObjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) 
						&& sp.getSubjectVar().getName().equals(pv)){
					return sp.getSubjectVar().getName();
				}
			}
		}
		
		return filterVar;
	}
	
	private static String GetDimensionFromGroupingVar(String grpVar, List<StatementPattern> stats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		String prefix_qb = "http://www.w3.org/2004/02/skos/core#narrower";
		List<String> possibleVars = new ArrayList<String>();
		for (StatementPattern sp : stats) {
			if(sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) && (sp.getObjectVar().getName().equals(grpVar) || sp.getSubjectVar().getName().equals(grpVar))) {
				return grpVar;
			}
			else if(sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) && (sp.getObjectVar().getName().equals(grpVar) || sp.getSubjectVar().getName().equals(grpVar))) {
				return grpVar;
			}
		}
		
		for (StatementPattern sp : stats) {
			if(!sp.getSubjectVar().isConstant() && sp.getObjectVar().getName().equals(grpVar)){
				possibleVars.add(sp.getSubjectVar().getName());
			}
		}
		for (String pv : possibleVars) {
			for (StatementPattern sp : stats) {
				if(!sp.getSubjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& sp.getObjectVar().getName().equals(pv)){
					return sp.getObjectVar().getName();
				}
				else if(!sp.getObjectVar().isConstant() && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) 
						&& sp.getSubjectVar().getName().equals(pv)){
					return sp.getSubjectVar().getName();
				}
			}
		}		
		return grpVar;
	}

	private static String GetRootVar(TupleExpr te){
		List<StatementPattern> stats = GetStatementPatterns(te);
		
		/// for each variable find the min distance to every other variable
		List<String> variables = GetAllVars(stats);
		//int[] varCosts = new int[variables.size()];
		//int i = 0;
		int minCost = 0;
		String rootVar = null;
		for (String varStart : variables) {
			int totalCost = 0;
			for (String varEnd : variables) {
				if(!varStart.equals(varEnd)){
					int cost = GetMinDistant(varStart, varEnd, stats);
					//varCosts[i] = cost;
					//i++;
					totalCost += cost;
				}
			}
			if(minCost == 0){
				rootVar = varStart;
				minCost = totalCost;
			}
			else if(totalCost < minCost){
				minCost = totalCost;
				rootVar = varStart;
			}
		}
		
		System.out.println("Root var: "+rootVar + ", cost " + minCost);
		return rootVar;
	}
	
	private static String GetRootVarUpdated(TupleExpr te){
		List<StatementPattern> stats = GetStatementPatterns(te);
		
		/// for each variable find the min distance to every other variable
		List<String> variables = GetAllVars(stats);
		String rootVar = null;
		
		for (String var : variables) {
			boolean isObjectVar = false;
			for (StatementPattern triple : stats) {
				if(triple.getObjectVar().getName().equals(var)){
					isObjectVar = true;
					break;
				}
			}
			if(!isObjectVar){
				rootVar = var;
				break;
			}
				
		}
		
		System.out.println("Root var: " + rootVar);
		return rootVar;
	}

	private static List<StatementPattern> GetDirectlyConnectedStatementPatterns(String var, List<StatementPattern> stats){
		List<StatementPattern> ptrns = new ArrayList<StatementPattern>();
		for (StatementPattern sp : stats) {
			if((sp.getSubjectVar().getName().equals(var) || sp.getObjectVar().getName().equals(var)) && !ptrns.contains(sp))
				ptrns.add(sp);
		}
		return ptrns;
	}
	
	private static List<String> GetConnectedPredicates(String var, List<StatementPattern> stats){
		List<String> prdk = new ArrayList<String>();
		for (StatementPattern sp : stats) {
			if((sp.getSubjectVar().getName().equals(var) || sp.getObjectVar().getName().equals(var)) && !prdk.contains(sp))
				prdk.add(sp.getPredicateVar().getValue().stringValue());
		}
		return prdk;
	}
	
	private static List<StatementPattern> GetDimensionStatementPatterns(String firstDimVar, List<StatementPattern> stats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		String prefix_qb = "http://www.w3.org/2004/02/skos/core#narrower";
		String varStart = firstDimVar;
		List<StatementPattern> ptrns = new ArrayList<StatementPattern>();
		boolean hasNext = true;
		//for (StatementPattern otherSP : stats) {
		while(hasNext) {
			hasNext = false;
			for (StatementPattern sp : stats) {
				if(sp.getSubjectVar().getName().equals(varStart)  && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& !sp.getObjectVar().isConstant()) {
					if(!ptrns.contains(sp)) 
						ptrns.add(sp);
					varStart = sp.getObjectVar().getName();
					hasNext = true;
					break;
				}
				else if(sp.getObjectVar().getName().equals(varStart)  && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) 
						&& !sp.getSubjectVar().isConstant()){
					if(!ptrns.contains(sp)) 
						ptrns.add(sp);
					varStart = sp.getSubjectVar().getName();
					hasNext = true;
					break;
				}
			}
		}		
		return ptrns;
	}
	
	private static List<StatementPattern> GetDimensionStatementPatterns(String firstDimVar, List<String> dimensionVars, List<StatementPattern> stats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		String varStart = firstDimVar;
		List<StatementPattern> ptrns = new ArrayList<StatementPattern>();
		for (int i=0; i<stats.size(); i++) {
			for (StatementPattern sp : stats) {
				if(sp.getSubjectVar().getName().equals(varStart)  && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& !sp.getObjectVar().isConstant()){
					if(!ptrns.contains(sp)) ptrns.add(sp);
					varStart = sp.getObjectVar().getName();
					if(dimensionVars.contains(varStart)) i = stats.size(); //// from first to last is found, stop searching
					break;
				}
			}
		}
		
		return ptrns;
	}
	
	private static String GetFirstConnectingPredicate(String lastDimVar, String rootVar, List<StatementPattern> stats){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		//String firstConnected = null;
		String varEnd = lastDimVar;
		for (StatementPattern sp : stats) {
			if(sp.getSubjectVar().getName().equals(rootVar) && sp.getObjectVar().getName().equals(varEnd))
				return sp.getPredicateVar().getValue().stringValue();
		}
		for (StatementPattern otherSP : stats) {
			if(otherSP.getSubjectVar().getName().equals(rootVar) && otherSP.getObjectVar().getName().equals(varEnd))
				return otherSP.getPredicateVar().getValue().stringValue();
			
			for (StatementPattern sp : stats) {
				if(sp.getObjectVar().getName().equals(varEnd)  && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) 
						&& !sp.getSubjectVar().isConstant()){
					varEnd = sp.getSubjectVar().getName();
					break;
				}
			}
		}
		
		return null;
	}
	
	private static boolean IsContainsAllDimensions(List<String> queryDims, List<String> matViewDims){
		boolean contains = true;
		for (String queryDim : queryDims) {
			if(!matViewDims.contains(queryDim)) {
				contains = false;
				break;
			}
		}
		return contains;
	}
	
	private static boolean IsContainsAllMeasures(List<String> queryMsrs, List<String> matViewMsrs){
		boolean contains = true;
		for (String queryMes : queryMsrs) {
			if(!matViewMsrs.contains(queryMes)) {
				contains = false;
				break;
			}
		}
		return contains;
	}
	
	private static boolean IsContainsAggregateInfo(AggregateInfo aggI, List<AggregateInfo> matViewMsrs){
		boolean contains = false;
		List<StatementPattern> qryMsrPatterns = aggI.getPatterns();
		//List<String> predicates = aggI.getPredicates();
		for (AggregateInfo viewAggregateInfo : matViewMsrs) {
			Map<String, String> qryToView = new HashMap<String, String>();
			String qryRoot =  qryMsrPatterns.get(0).getSubjectVar().getName();
			List<StatementPattern> viewMsrPatterns = viewAggregateInfo.getPatterns();
			String viewRoot =  viewMsrPatterns.get(0).getSubjectVar().getName();
			qryToView.put(qryRoot, viewRoot);
			for (StatementPattern qryMsrTriple : qryMsrPatterns) {
				StatementPattern viewTriple = GetStatementPattern(viewRoot, qryMsrTriple.getPredicateVar().getValue().stringValue(), null, viewMsrPatterns);
				if(viewTriple != null){
					qryToView.put(qryMsrTriple.getObjectVar().getName(), viewTriple.getObjectVar().getName());
				}
			}
			String aggeeString = aggI.getFunction().toString();
			String vieweeString = viewAggregateInfo.getFunction().toString();
			vieweeString = vieweeString.replace(viewAggregateInfo.getNameVar(), aggI.getNameVar());
			for (String key : qryToView.keySet()) {
				String varInView = qryToView.get(key);
				vieweeString = vieweeString.replace(varInView, key);
			}
			if(aggeeString.equalsIgnoreCase(vieweeString))
				contains = true;
			//String modifiedVieweeString = 
		}
		return contains;
	}
	
	private static Map<AggregateInfo, AggregateInfo> /*List<AggregateInfo>*/ getSameAggregates(List<AggregateInfo> queryMsrs, List<AggregateInfo> matViewMsrs){
		//List<AggregateInfo> retList = new ArrayList<AggregateInfo>();
		Map<AggregateInfo, AggregateInfo> viewToQryAggInfo = new HashMap<AggregateInfo, AggregateInfo>();
		for(AggregateInfo aggI : queryMsrs){
			List<StatementPattern> qryMsrPatterns = aggI.getPatterns();
			//List<String> predicates = aggI.getPredicates();
			for (AggregateInfo viewAggregateInfo : matViewMsrs) {
				Map<String, String> qryToView = new HashMap<String, String>();
				String qryRoot =  qryMsrPatterns.get(0).getSubjectVar().getName();
				List<StatementPattern> viewMsrPatterns = viewAggregateInfo.getPatterns();
				String viewRoot =  viewMsrPatterns.get(0).getSubjectVar().getName();
				qryToView.put(qryRoot, viewRoot);
				for (StatementPattern qryMsrTriple : qryMsrPatterns) {
					StatementPattern viewTriple = GetStatementPattern(viewRoot, qryMsrTriple.getPredicateVar().getValue().stringValue(), null, viewMsrPatterns);
					if(viewTriple != null){
						qryToView.put(qryMsrTriple.getObjectVar().getName(), viewTriple.getObjectVar().getName());
					}
				}
				String aggeeString = aggI.getFunction().toString();
				String vieweeString = viewAggregateInfo.getFunction().toString();
				vieweeString = vieweeString.replace(viewAggregateInfo.getNameVar(), aggI.getNameVar());
				for (String key : qryToView.keySet()) {
					String varInView = qryToView.get(key);
					vieweeString = vieweeString.replace(varInView, key);
				}
				if(aggeeString.equalsIgnoreCase(vieweeString)){
					//retList.add(viewAggregateInfo);
					viewToQryAggInfo.put(viewAggregateInfo, aggI);
					break;
				}
			}
		}
		return viewToQryAggInfo;//retList;
	}
	
	private static Map<String, String> getViewToQueryDimensionMapping(List<DimensionInfo> queryDims, List<DimensionInfo> viewDims){
		//List<AggregateInfo> retList = new ArrayList<AggregateInfo>();
		Map<String, String> viewToQryDimInfo = new HashMap<String, String>();
		for(DimensionInfo dimI : queryDims){
			//List<String> predicates = aggI.getPredicates();
			for (DimensionInfo viewDimInfo : viewDims) {
				if(!dimI.getPredicate().equals(viewDimInfo.getPredicate())) 
					continue;
				
				List<StatementPattern> viewDimPatterns = viewDimInfo.getPatterns();
				//Map<String, String> qryToView = new HashMap<String, String>();
				if(viewDimPatterns == null || viewDimPatterns.size() == 0){ ////only the first pattern exist
					viewToQryDimInfo.put(viewDimInfo.getFirstPattern().getObjectVar().getName(), dimI.getFirstPattern().getObjectVar().getName());
					break;
				}
				//int i = 0;
				StatementPattern lastViewSP = viewDimPatterns.get(viewDimPatterns.size() - 1);
				StatementPattern correspondingQuerySP = dimI.getPatterns().get(viewDimPatterns.size() - 1);
				//////////////// ################### IDEALLY NEED TO CHECK UNTIL SELECTEDVAR IN DIMENSION
				viewToQryDimInfo.put(lastViewSP.getObjectVar().getName(), correspondingQuerySP.getObjectVar().getName());
			}
		}
		return viewToQryDimInfo;
	}
	
	private static boolean IsProperLevel(List<StatementPattern> queryDimStats, List<StatementPattern> matViewDimStats){
		return matViewDimStats.size() <= queryDimStats.size();
	}
	
	private static boolean IsProperLevel(DimensionInfo queryDI, DimensionInfo viewDI){//// this is too primitive - need to check by selected hierarchy
		if(queryDI.getPatterns() != null  && viewDI.getPatterns() == null)
			return false;
		else if(queryDI.getPatterns() == null  && viewDI.getPatterns() != null)
			return true;
		else if(queryDI.getPatterns() != null && queryDI.getPatterns().size() > 0 && viewDI.getPatterns() != null && viewDI.getPatterns().size() > 0){
			String qrySelVar = queryDI.getSelectedDimVar();
			String viewSelVar = viewDI.getSelectedDimVar();
			int qryLevel = 0;
			for (StatementPattern qryTriple : queryDI.getPatterns()) {
				qryLevel++;
				if(qryTriple.getObjectVar().getName().equals(qrySelVar))
					break;
			}
			int viewLevel = 0;
			for (StatementPattern viewTriple : viewDI.getPatterns()) {
				viewLevel++;
				if(viewTriple.getObjectVar().getName().equals(viewSelVar))
					break;
			}
			return viewLevel <= qryLevel;///viewDI.getPatterns().size() <= queryDI.getPatterns().size();
		}
		return false;
	}
	
	private static boolean IsContainsSameConstants(String varQuery, List<StatementPattern> qryPtrns, String varMV, List<StatementPattern> viewPtrns){
		List<StatementPattern> queryDCPtrns = GetPatternsWithConstants(varQuery, qryPtrns);
		List<StatementPattern> viewDCPtrns = GetPatternsWithConstants(varMV, viewPtrns);
		for (StatementPattern qrySP : queryDCPtrns) {
			boolean contains = false;
			for (StatementPattern viewSP : viewDCPtrns) {
				if(qrySP.getSubjectVar().getName().equals(varQuery) && viewSP.getSubjectVar().getName().equals(varMV) &&
						qrySP.getObjectVar().getValue().stringValue().equals(viewSP.getObjectVar().getValue().stringValue())){
					contains = true;
					break;
				}
			}
			if(!contains) 
				return false;
		}
		return true;
	}
	
	private static List<List<StatementPattern>> DivideQueryBasedOnMV(String varQueryRoot, List<StatementPattern> qryPtrns, String varMVRoot, 
			List<StatementPattern> viewPtrns, List<String> dimensionVars){
		
		List<List<StatementPattern>> retList = new ArrayList<List<StatementPattern>>();
		
		List<StatementPattern> ptrnsToDelete = new ArrayList<StatementPattern>();
		List<StatementPattern> ptrnsToKeep = CopyList(qryPtrns);
		if(!IsContainsSameConstants(varQueryRoot, qryPtrns, varMVRoot, viewPtrns))
			return null;
		ptrnsToDelete.addAll(GetPatternsWithConstants(varQueryRoot, qryPtrns));
		for (String dim : dimensionVars) {
			// get dimension predicate
			String dimPredicate = GetFirstConnectingPredicate(dim, varQueryRoot, qryPtrns);
			// get pattern from root to first dimension var
			StatementPattern firstDimPtrnQry = GetStatementPattern(varQueryRoot, dimPredicate, qryPtrns);
			StatementPattern firstDimPtrnMV = GetStatementPattern(varMVRoot, dimPredicate, viewPtrns);
			ptrnsToDelete.add(firstDimPtrnQry);
			
			String higherLevelVarMV = GetHigherLevelVar(firstDimPtrnMV.getObjectVar().getName(), viewPtrns);
			String higherLevelVarQuery = GetHigherLevelVar(firstDimPtrnQry.getObjectVar().getName(), qryPtrns);
			while(higherLevelVarMV != null){
				//// remove these patterns from the list
				ptrnsToDelete.add(GetCurrentLevelStatementPattern(higherLevelVarQuery, qryPtrns));
				////get patterns with constants and compare with query
				if(!IsContainsSameConstants(higherLevelVarQuery, qryPtrns, higherLevelVarMV, viewPtrns))
					return null;
				
				////copy these patterns to list of deleted patterns for query
				ptrnsToDelete.addAll(GetPatternsWithConstants(higherLevelVarQuery, qryPtrns));
				
				////get the next level
				higherLevelVarMV = GetHigherLevelVar(higherLevelVarMV, viewPtrns);
				higherLevelVarQuery = GetHigherLevelVar(higherLevelVarQuery, qryPtrns);
			}
			ptrnsToKeep.removeAll(ptrnsToDelete);
			System.out.println(ptrnsToDelete.size()+ptrnsToKeep.size());
			retList.add(ptrnsToKeep);
			retList.add(ptrnsToDelete);
		}
		return retList;
	}
	
	private static List<List<StatementPattern>> separateQueryTriplesBasedOnView(String queryRootVar, List<StatementPattern> qryPtrns, String viewRootVar, 
			List<StatementPattern> viewPtrns, List<DimensionInfo> queryDims){	
		
		List<List<StatementPattern>> retList = new ArrayList<List<StatementPattern>>();		
		List<StatementPattern> ptrnsToDelete = new ArrayList<StatementPattern>();
		List<StatementPattern> ptrnsToKeep = CopyList(qryPtrns);
		if(!IsContainsSameConstants(queryRootVar, qryPtrns, viewRootVar, viewPtrns))
			return null;
		ptrnsToDelete.addAll(GetPatternsWithConstants(queryRootVar, qryPtrns));
		for (DimensionInfo dimInfo : queryDims) {
			String dimPredicate = dimInfo.getPredicate();
			// get pattern from root to first dimension var
			StatementPattern firstDimPtrnQry = dimInfo.getFirstPattern();//GetStatementPattern(varQueryRoot, dimPredicate, qryPtrns);
			//StatementPattern viewDimInfo = 
			
			StatementPattern firstDimPtrnMV = GetStatementPattern(viewRootVar, dimPredicate, null, viewPtrns);//GetStatementPattern(varMVRoot, dimPredicate, viewPtrns);
			ptrnsToDelete.add(firstDimPtrnQry);
			
			String higherLevelVarMV = GetHigherLevelVar(firstDimPtrnMV.getObjectVar().getName(), viewPtrns);
			String higherLevelVarQuery = GetHigherLevelVar(firstDimPtrnQry.getObjectVar().getName(), qryPtrns);
			while(higherLevelVarMV != null){
				//// remove these patterns from the list
				ptrnsToDelete.add(GetCurrentLevelStatementPattern(higherLevelVarQuery, dimInfo.getPatterns())); //qryPtrns));
				////get patterns with constants and compare with query
				if(!IsContainsSameConstants(higherLevelVarQuery, qryPtrns, higherLevelVarMV, viewPtrns))
					return null;
				
				////copy these patterns to list of deleted patterns for query
				ptrnsToDelete.addAll(GetPatternsWithConstants(higherLevelVarQuery, qryPtrns));
				
				////get the next level
				higherLevelVarMV = GetHigherLevelVar(higherLevelVarMV, viewPtrns);
				higherLevelVarQuery = GetHigherLevelVar(higherLevelVarQuery, qryPtrns);
			}
			ptrnsToKeep.removeAll(ptrnsToDelete);
			System.out.println("Separation: delete triples: " + ptrnsToDelete.size() + ", keep triples: " + ptrnsToKeep.size());
			retList.add(ptrnsToKeep);
			retList.add(ptrnsToDelete);
		}
		return retList;
	}
	
	private static Object[] GetRewrittenMeasures(String varQueryRoot, List<StatementPattern> qryPtrns, List<String> measureVars, 
			String varViewRoot, List<StatementPattern> viewPtrns, List<ExtensionElem> qrySelectMeasureElems, 
			List<ExtensionElem> viewSelectMeasureElems, List<ExtensionElem> viewConstructExtElems, List<ProjectionElemList> prjctns){
		List<String> newMeasures = new ArrayList<String>();
		Set<String> vars = new HashSet<String>(measureVars);
		List<StatementPattern> ptrnToDelete = new ArrayList<StatementPattern>();
		List<StatementPattern> ptrnToAdd = new ArrayList<StatementPattern>();
		List<ExtensionElem> eeFromMVSelect =  new ArrayList<ExtensionElem>();
		Map<String, String> qryMVDict = new HashMap<String, String>();
		Map<ExtensionElem, ExtensionElem> mvQryEEMap = new HashMap<ExtensionElem, ExtensionElem>();
		for (String qryMeasureVarName : vars) {
			//// find in the query and add to deleted statements
			for (StatementPattern sp : qryPtrns) {
				if(sp.getSubjectVar().getName().equals(varQueryRoot) && sp.getObjectVar().getName().equals(qryMeasureVarName)){
					if(!ptrnToDelete.contains(sp))
						ptrnToDelete.add(sp);
					break;
				}
			}
			//matview measure from select has the following name:
			String matViewMeasureVarName = GetCounterpartMeasureVarNameFromView(varQueryRoot, qryPtrns, varViewRoot,  viewPtrns, qryMeasureVarName);
			for (StatementPattern sp : viewPtrns) {
				if(sp.getSubjectVar().getName().equals(varViewRoot) && sp.getObjectVar().getName().equals(matViewMeasureVarName)){
					if(!ptrnToAdd.contains(sp))
						ptrnToAdd.add(sp);
					break;
				}
			}
			qryMVDict.put(qryMeasureVarName, matViewMeasureVarName);
			//// get functions from ExtElem with external names
			for (ExtensionElem ee : qrySelectMeasureElems) {
				if(ee.toString().contains(qryMeasureVarName)){//// ext element contains this measure
					ValueExpr veQry = ee.getExpr();
					for (ExtensionElem eeMV : viewSelectMeasureElems) {
						ValueExpr veMV = eeMV.getExpr();
						String mvVEStrReplaced = veMV.toString().replace(matViewMeasureVarName, qryMeasureVarName);
						String qryVEStr = veQry.toString();
						if(mvVEStrReplaced.equals(qryVEStr)){
							if(!eeFromMVSelect.contains(eeMV)) {
								eeFromMVSelect.add(eeMV);
								mvQryEEMap.put(eeMV, ee);
							}
							break;
						}
					}
				}
			}
		}
		//// for each identified ee take a projection from the construct query and add it to patterns to add
		for (ExtensionElem eeMV : eeFromMVSelect) {
			for (ProjectionElemList peList : prjctns) {
				for (ProjectionElem pe : peList.getElements()) {
					if(pe.getTargetName().equals("object") && pe.getSourceName().equals(eeMV.getName())){
						for (ExtensionElem eeConstr : viewConstructExtElems) {
							if(eeConstr.getName().equals(peList.getElements().get(1).getSourceName())){
								String prdct = ((ValueConstant)eeConstr.getExpr()).getValue().stringValue();
								System.out.println("predicate is " + prdct); //peList.getElements().get(1).getSourceName());
								System.out.println("Triple:  ?" + varQueryRoot + " <" + prdct + "> ?" + eeMV.getName() + " ."); //peList.getElements().get(1).getSourceName());
							}
						}
						
						
						if(eeMV.getExpr() instanceof Sum){
							newMeasures.add("(SUM(?"+ eeMV.getName() + ") AS ?" + mvQryEEMap.get(eeMV).getName() + ")");
						}
						else if(eeMV.getExpr() instanceof Count){
							newMeasures.add("(COUNT(?"+ eeMV.getName() + ") AS ?" + mvQryEEMap.get(eeMV).getName() + ")");
						}
						else if(eeMV.getExpr() instanceof Min){
							newMeasures.add("(MIN(?"+ eeMV.getName() + ") AS ?" + mvQryEEMap.get(eeMV).getName() + ")");
						}
						else if(eeMV.getExpr() instanceof Max){
							newMeasures.add("(MAX(?"+ eeMV.getName() + ") AS ?" + mvQryEEMap.get(eeMV).getName() + ")");
						}
					}
				}
			}
		}
		
		Map<String, String> qryMVAggDict = new HashMap<String, String>();
		for (ExtensionElem eeMV : mvQryEEMap.keySet()) {
			ExtensionElem qryEE = mvQryEEMap.get(eeMV);
			qryMVAggDict.put(qryEE.getName(), eeMV.getName());
		}
		
		Object[] retObj = new Object[4];
		retObj[0] = newMeasures;
		retObj[1] = ptrnToDelete;
		retObj[2] = ptrnToAdd;
		retObj[3] = qryMVAggDict;
		return retObj;
	}
	
	private static List<String> GetMVConstructElements(TupleExpr teMV, List<String> vars){
		//teMV.toString();
		List<String> mvTuples = new ArrayList<String>();
		List<ProjectionElemList> neededProjs = new ArrayList<ProjectionElemList>();
		List<ExtensionElem> elements = null;
		if(teMV instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)teMV;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof MultiProjection){
					MultiProjection mp = (MultiProjection)uto;
					List<ProjectionElemList> prjcts = mp.getProjections();
					for (ProjectionElemList projElemList : prjcts) {
						for (ProjectionElem projElem : projElemList.getElements()) {
							if(projElem.getTargetName().equals("object") && vars.contains(projElem.getSourceName())){
								neededProjs.add(projElemList);
								System.out.println(projElem.getSourceName() + " - " + projElem.getTargetName());
							}
							else if(projElem.getTargetName().equals("object") && projElem.getSourceName().startsWith("_const"))
								neededProjs.add(projElemList);
						}
						
					}
					Extension ext = (Extension)mp.getArg();
					elements = ext.getElements(); //// values for predicates and objects					
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		if(elements != null && neededProjs.size() > 0){
			for (ProjectionElemList pel : neededProjs) {
				String subj = null; String predicate = null; String obj = null;
				for (ProjectionElem projElem : pel.getElements()) {
					if(projElem.getTargetName().equals("object") ){ //&& vars.contains(projElem.getSourceName())
						obj = projElem.getSourceName();
					}
					else if(projElem.getTargetName().equals("subject")){
						subj = projElem.getSourceName();
					}
					else if(projElem.getTargetName().equals("predicate")){
						predicate = projElem.getSourceName();
					}
				}
				//if(subj != null && predicate != null && obj != null){
					String predStr = GetValueFromExtensionElem(predicate, elements);
					String objStr = GetValueFromExtensionElem(obj, elements); 
					mvTuples.add("?" + subj + " <" + predStr + "> " + ((objStr == null) ? ("?" + obj) : ("<" + objStr + ">")));
				//}
			}
		}
		return mvTuples;
	}
	
	private static List<String> getRewrittenGraphTriples(TupleExpr viewTE, Map<String, String> viewToQueryDimMapping){
		//teMV.toString();
		List<String> vars = new ArrayList<String>(viewToQueryDimMapping.keySet());
		List<String> mvTuples = new ArrayList<String>();
		List<ProjectionElemList> neededProjs = new ArrayList<ProjectionElemList>();
		List<ExtensionElem> elements = null;
		if(viewTE instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)viewTE;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof MultiProjection){
					MultiProjection mp = (MultiProjection)uto;
					List<ProjectionElemList> prjcts = mp.getProjections();
					for (ProjectionElemList projElemList : prjcts) {
						for (ProjectionElem projElem : projElemList.getElements()) {
							if(projElem.getTargetName().equals("object") && vars.contains(projElem.getSourceName())){
								neededProjs.add(projElemList);
								System.out.println(projElem.getSourceName() + " - " + projElem.getTargetName());
							}
							else if(projElem.getTargetName().equals("object") && projElem.getSourceName().startsWith("_const"))
								neededProjs.add(projElemList);
						}
						
					}
					Extension ext = (Extension)mp.getArg();
					elements = ext.getElements(); //// values for predicates and objects
					break;
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		if(elements != null && neededProjs.size() > 0){
			for (ProjectionElemList pel : neededProjs) {
				String subj = null; String predicate = null; String obj = null;
				for (ProjectionElem projElem : pel.getElements()) {
					if(projElem.getTargetName().equals("object") ){ //&& vars.contains(projElem.getSourceName())
						obj = projElem.getSourceName();
					}
					else if(projElem.getTargetName().equals("subject")){
						subj = projElem.getSourceName();
					}
					else if(projElem.getTargetName().equals("predicate")){
						predicate = projElem.getSourceName();
					}
				}
				//if(subj != null && predicate != null && obj != null){
					String predStr = GetValueFromExtensionElem(predicate, elements);
					String objStr = GetValueFromExtensionElem(obj, elements); 
					mvTuples.add("?" + subj + " <" + predStr + "> " + ((objStr == null) ? ("?" + viewToQueryDimMapping.get(obj)) : ("<" + objStr + ">")) + " .");
				//}
			}
		}
		return mvTuples;
	}
	
	private static List<String> getRewrittenGraphTriplesForAggregates(TupleExpr viewTE, List<AggregateInfo> viewAggregates){
		List<String> vars = new ArrayList<String>();
		for (AggregateInfo viewAggInfo : viewAggregates) {
			vars.add(viewAggInfo.getFunction().getName());
		}
		List<String> mvTuples = new ArrayList<String>();
		List<ProjectionElemList> neededProjs = new ArrayList<ProjectionElemList>();
		List<ExtensionElem> elements = null;
		if(viewTE instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)viewTE;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof MultiProjection){
					MultiProjection mp = (MultiProjection)uto;
					List<ProjectionElemList> prjcts = mp.getProjections();
					for (ProjectionElemList projElemList : prjcts) {
						for (ProjectionElem projElem : projElemList.getElements()) {
							if(projElem.getTargetName().equals("object") && vars.contains(projElem.getSourceName())){
								neededProjs.add(projElemList);
								System.out.println(projElem.getSourceName() + " - " + projElem.getTargetName());
							}
						}
					}
					Extension ext = (Extension)mp.getArg();
					elements = ext.getElements(); //// values for predicates and objects
					break;
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
		}
		if(elements != null && neededProjs.size() > 0){
			for (ProjectionElemList pel : neededProjs) {
				String subj = null; String predicate = null; String obj = null;
				for (ProjectionElem projElem : pel.getElements()) {
					if(projElem.getTargetName().equals("object") ){ //&& vars.contains(projElem.getSourceName())
						obj = projElem.getSourceName();
					}
					else if(projElem.getTargetName().equals("subject")){
						subj = projElem.getSourceName();
					}
					else if(projElem.getTargetName().equals("predicate")){
						predicate = projElem.getSourceName();
					}
				}
				//if(subj != null && predicate != null && obj != null){
					String predStr = GetValueFromExtensionElem(predicate, elements);
					String objStr = GetValueFromExtensionElem(obj, elements); 
					mvTuples.add("?" + subj + " <" + predStr + "> " + ((objStr == null) ? ("?" + obj) : ("<" + objStr + ">")) + " .");
				//}
			}
		}
		return mvTuples;
	}
	
	private static String GetFilterString(BinaryValueOperator bvo){
		if(bvo instanceof Compare){
			Compare comp = (Compare)bvo;
			String leftArg = "?" + ((Var)comp.getLeftArg()).getName();
			String operator = comp.getOperator().getSymbol();
			String rightArg = null;
			if(comp.getRightArg() instanceof ValueConstant){
				Value v = ((ValueConstant)comp.getRightArg()).getValue();
				if(v instanceof LiteralImpl){ //// http://www.w3.org/2001/XMLSchema#integer //// http://www.w3.org/2001/XMLSchema#string
					LiteralImpl li = (LiteralImpl)v;
					if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#string"))
						rightArg = "\"" + v.stringValue() + "\"";
					else if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#integer"))
						rightArg = v.stringValue();
					else if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#decimal"))
						rightArg = "\"" + v.stringValue() + "\"^^http://www.w3.org/2001/XMLSchema#decimal" ;
				}
			}
			else if (comp.getRightArg() instanceof Var)
				rightArg = ((Var)comp.getRightArg()).getName();
			return leftArg + " " + operator + " " + rightArg;
		}
		else if (bvo instanceof And){
			boolean needParent = false;
			And comp = (And)bvo;
			String leftArgStr = null; 
			ValueExpr leftArg = comp.getLeftArg();
			if(leftArg instanceof BinaryValueOperator){ 
				leftArgStr = GetFilterString((BinaryValueOperator)leftArg);
				needParent = true;
			}
			else if (leftArg instanceof Var)
				leftArgStr = ((Var)leftArg).getName();
			String operator = "&&"; //// AND
			String rightArgStr = null;
			ValueExpr rightArg = comp.getRightArg();
			if(rightArg instanceof BinaryValueOperator) {
				rightArgStr = GetFilterString((BinaryValueOperator)rightArg);
				needParent = true;
			}
//			else if (rightArg instanceof Var)
//				rightArgStr = ((Var)rightArg).getName();
//			else if(rightArg instanceof ValueConstant){
//				Value v = ((ValueConstant)comp.getRightArg()).getValue();
//				if(v instanceof LiteralImpl){ //// http://www.w3.org/2001/XMLSchema#integer //// http://www.w3.org/2001/XMLSchema#string
//					LiteralImpl li = (LiteralImpl)v;
//					if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#string"))
//						rightArgStr = "\"" + v.stringValue() + "\"";
//					else if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#integer"))
//						rightArgStr = v.stringValue();
//					else if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#decimal"))
//						rightArgStr = "\"" + v.stringValue() + "\"^^http://www.w3.org/2001/XMLSchema#decimal" ;
//				}
//			}
			if(needParent) return "(" + leftArgStr + " " + operator + " " + rightArgStr + ")";
			else return leftArgStr + " " + operator + " " + rightArgStr;
		}
		else if (bvo instanceof Or){
			boolean needParent = false;
			Or comp = (Or)bvo;
			String leftArgStr = null; 
			ValueExpr leftArg = comp.getLeftArg();
			if(leftArg instanceof BinaryValueOperator) {
				leftArgStr = GetFilterString((BinaryValueOperator)leftArg);
				needParent = true;
			}
			else if (leftArg instanceof Var)
				leftArgStr = ((Var)leftArg).getName();
			String operator = "||"; ////OR
			String rightArgStr = null;
			ValueExpr rightArg = comp.getRightArg();
			if(rightArg instanceof BinaryValueOperator){ 
				rightArgStr = GetFilterString((BinaryValueOperator)rightArg);
				needParent = true;
			}
//			else if (rightArg instanceof Var)
//				rightArgStr = ((Var)rightArg).getName();
//			else if(rightArg instanceof ValueConstant){
//				Value v = ((ValueConstant)comp.getRightArg()).getValue();
//				if(v instanceof LiteralImpl){ //// http://www.w3.org/2001/XMLSchema#integer //// http://www.w3.org/2001/XMLSchema#string
//					LiteralImpl li = (LiteralImpl)v;
//					if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#string"))
//						rightArgStr = "\"" + v.stringValue() + "\"";
//					else if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#integer"))
//						rightArgStr = v.stringValue();
//					else if(li.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#decimal"))
//						rightArgStr = "\"" + v.stringValue() + "\"^^http://www.w3.org/2001/XMLSchema#decimal" ;
//				}
//			}
			if(needParent) return "(" + leftArgStr + " " + operator + " " + rightArgStr + ")";
			else return leftArgStr + " " + operator + " " + rightArgStr;
		}
		return null;
	}
	
	private static List<String> GetFilterStrings(Filter f){
		List<String> filterStrings = new ArrayList<String>();
		TupleExpr uto = (UnaryTupleOperator)f;
		while (uto instanceof Filter){
			ValueExpr ve = ((Filter)uto).getCondition();
			if(ve instanceof BinaryValueOperator)
				filterStrings.add(GetFilterString((BinaryValueOperator)ve));
			
			uto = ((UnaryTupleOperator)uto).getArg();
		}
		return filterStrings;
	}
	
	////helpers
	private static StatementPattern GetStatementPattern(String subj, String pred, List<StatementPattern> ptrns){
		for (StatementPattern sp : ptrns) {
			if(sp.getSubjectVar().getName().equals(subj) && sp.getPredicateVar().getValue().stringValue().equals(pred))
				return sp;
		}
		return null;
	}
	
	private static StatementPattern GetStatementPattern(String subj, String pred, String obj, List<StatementPattern> ptrns){
		for (StatementPattern sp : ptrns) {
			if(obj == null){
				if(sp.getSubjectVar().getName().equals(subj) && sp.getPredicateVar().getValue().stringValue().equals(pred))
					return sp;
			}
			else if(subj == null){
				if(sp.getObjectVar().getName().equals(obj) && sp.getPredicateVar().getValue().stringValue().equals(pred))
					return sp;
			}
			else if(pred == null){
				if(sp.getSubjectVar().getName().equals(subj) && sp.getObjectVar().getName().equals(obj))
					return sp;
			}
			else
				if(sp.getSubjectVar().getName().equals(subj) && sp.getPredicateVar().getValue().stringValue().equals(pred) && sp.getObjectVar().getName().equals(obj))
					return sp;
		}
		return null;
	}
	
	private static StatementPattern GetCurrentLevelStatementPattern(String subj, List<StatementPattern> ptrns){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		String prefix_qb = "http://www.w3.org/2004/02/skos/core#narrower";
		for (StatementPattern sp : ptrns) {
			if(sp.getObjectVar().getName().equals(subj) && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) &&
					!sp.getSubjectVar().isConstant())
				return sp;
			else if(sp.getSubjectVar().getName().equals(subj) && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) &&
					!sp.getObjectVar().isConstant())
				return sp;
		}
		return null;
	}
	
	private static String GetHigherLevelVar(String varOfLevel, List<StatementPattern> ptrns){
		String prefix_qb4o = "http://www.w3.org/2004/02/skos/core#broader";
		String prefix_qb = "http://www.w3.org/2004/02/skos/core#narrower";
		for (StatementPattern sp : ptrns) {
			if(sp.getSubjectVar().getName().equals(varOfLevel) && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb4o) &&
					!sp.getObjectVar().isConstant())
				return sp.getObjectVar().getName();
			else if(sp.getObjectVar().getName().equals(varOfLevel) && sp.getPredicateVar().getValue().stringValue().equals(prefix_qb) &&
					!sp.getSubjectVar().isConstant())
				return sp.getSubjectVar().getName();
		}
		return null;
	}
	
	private static List<StatementPattern> GetPatternsWithConstants(String subj, List<StatementPattern> ptrns){
		List<StatementPattern> lst = new ArrayList<StatementPattern>();
		for (StatementPattern sp : ptrns) {
			if(sp.getSubjectVar().getName().equals(subj) && sp.getObjectVar().isConstant())
				lst.add(sp);
		}
		return lst;
	}
	
	private static List<StatementPattern> CopyList(List<StatementPattern> ptrns){
		List<StatementPattern> lst = new ArrayList<StatementPattern>();
		lst.addAll(ptrns);
		return lst;
	}

	private static String GetCounterpartMeasureVarNameFromView(String qryRootNode, List<StatementPattern> qryPtrns, String varMVRoot, 
			List<StatementPattern> viewPtrns, String measureVar){
		StatementPattern qrySP = GetStatementPattern(qryRootNode, null, measureVar, qryPtrns);//// measure predicate
		StatementPattern mvSP = GetStatementPattern(varMVRoot, qrySP.getPredicateVar().getValue().stringValue(), null, viewPtrns);//// measure from matview
		if(mvSP != null)
			return mvSP.getObjectVar().getName();
		return null;
	}
	
	private static Object[] GetCounterpartVarNamesQueryAndView(String qryRootNode, List<StatementPattern> qryPtrns, 
			String mvRootNode, List<StatementPattern> viewPtrns){
		Map<String, String> viewToQuery = new HashMap<String, String>();
		Map<String, String> queryToView = new HashMap<String, String>();
		viewToQuery.put(mvRootNode, qryRootNode);
		queryToView.put(qryRootNode, mvRootNode);
		boolean wasPut = true;
		List<String> viewVars = new ArrayList<String>(); 
		viewVars.add(mvRootNode);
		while(wasPut){
			wasPut = false;
			List<String> newViewVars = new ArrayList<String>(); 
			for (String viewVar : viewVars) {
				String qryVar = viewToQuery.get(viewVar);
				for (StatementPattern pattern : viewPtrns) {
					if(!pattern.getSubjectVar().isConstant() && !pattern.getObjectVar().isConstant() && pattern.getSubjectVar().getName().equals(viewVar)){
						StatementPattern qrySP = GetStatementPattern(qryVar, pattern.getPredicateVar().getValue().stringValue(), qryPtrns);
						if(qrySP != null){
							viewToQuery.put(pattern.getObjectVar().getName(), qrySP.getObjectVar().getName());
							queryToView.put(qrySP.getObjectVar().getName(), pattern.getObjectVar().getName());
							if(!newViewVars.contains(pattern.getObjectVar().getName()))
								newViewVars.add(pattern.getObjectVar().getName());
							wasPut = true;
						}
					}
				}
			}
			if(wasPut) viewVars = newViewVars;
		}
		//System.out.println(viewToQuery);
		Object[] retObj = new Object[2];
		retObj[0] = queryToView;
		retObj[1] = viewToQuery;
		return retObj;
	}
	
	private static String GetValueFromExtensionElem(String name, List<ExtensionElem> elements){
		for (ExtensionElem ee : elements) {
			if(ee.getName().equals(name)){
				ValueExpr ve = ee.getExpr();
				if(ve instanceof ValueConstant){
					ValueConstant vc = (ValueConstant)ve;
					return vc.getValue().stringValue();
				}
			}
		}
		return null;
	}
	
	private static List<DimensionInfo> getQueryDimensions(TupleExpr te){
		List<DimensionInfo> dimensions = new ArrayList<DimensionInfo>();
		List<String> queryMsrPredicates = new ArrayList<String>();
		List<String> queryDimPredicates = new ArrayList<String>();
		List<String> queryMsrVarsStr = GetQueryMeasureVars(te);
		queryMsrVarsStr = new ArrayList<String>(new HashSet<String>(queryMsrVarsStr));//// get rid of same values
		String queryRootNode = GetRootVarUpdated(te);///////$####################$$$$$$$$$$$$$$$$   CHANGE TO GET IS AS SUBJECT
		List<StatementPattern> qryGPTriples =  GetStatementPatterns(te);
		List<StatementPattern> rootConnectedTriples = GetDirectlyConnectedStatementPatterns(queryRootNode, qryGPTriples);
		for (StatementPattern triple : rootConnectedTriples) {
			if(!triple.getObjectVar().isConstant() && queryMsrVarsStr.contains(triple.getObjectVar().getName()))
				queryMsrPredicates.add(triple.getPredicateVar().getValue().stringValue());
			else if(!triple.getObjectVar().isConstant())
				queryDimPredicates.add(triple.getPredicateVar().getValue().stringValue());
		}
		//// 
		List<String> grpVars = GetGroupingVars(te); //// query groupby variables
		List<String> dimGrpVars = new ArrayList<String>(); //// hierarchical variables from groupby (group by may include vars that are not directly hierarchical but connected to H)
		for (String grpVar : grpVars) {
			dimGrpVars.add(GetDimensionFromGroupingVar(grpVar, qryGPTriples)); ///for now group by variables 
		}//// now dimGrpVars contain variables belonging to hierarchies
		
		List<String> filterVars = GetFilterVars(te); //// query filter variables
		List<String> dimFilterVars = new ArrayList<String>(); //// hierarchical variables related to FILTERs
		for (String fltr : filterVars) {
			dimFilterVars.add(GetDimensionFromFilterVar(fltr, qryGPTriples));
		}
		
		for (String predicate : queryDimPredicates) {
			StatementPattern firstPattern = GetStatementPattern(queryRootNode, predicate, null, qryGPTriples);
			List<StatementPattern> dimTriples = GetDimensionStatementPatterns(firstPattern.getObjectVar().getName(), qryGPTriples);
			List<String> dimensionVars = GetAllVars(dimTriples);
			boolean isKept = false;
			String groupByVar = null;
			for (String grpVar : dimGrpVars) {
				if(dimensionVars.contains(grpVar)){
					isKept = true;
					groupByVar = grpVar;
				}
			}
			String filterVar = null;
			for (String fltVar : dimFilterVars) {
				if(dimensionVars.contains(fltVar)){
					filterVar = fltVar;
				}
			}
			DimensionInfo dimInfo = new DimensionInfo(predicate, dimTriples, firstPattern, isKept, groupByVar, filterVar);
			dimensions.add(dimInfo);
		}
		return dimensions;
	}
	
	private static List<AggregateInfo> getQueryMeasures(TupleExpr te){
		List<AggregateInfo> measures = new ArrayList<AggregateInfo>();
		
		String queryRootNode = GetRootVarUpdated(te);
		List<StatementPattern> qryGPTriples =  GetStatementPatterns(te);
		List<StatementPattern> rootConnectedTriples = GetDirectlyConnectedStatementPatterns(queryRootNode, qryGPTriples);
				
		List<ExtensionElem> extElems = GetMeasureElems(te);
		for (ExtensionElem ee : extElems) {
			Set<String> variables = new HashSet<String>();
			String aggregateName = ee.getName();
			List<String> queryMsrPredicates = new ArrayList<String>();
			List<StatementPattern> queryMsrTriples = new ArrayList<StatementPattern>();
			ValueExpr ve = ee.getExpr();
			if(ve instanceof Sum){
				ValueExpr innerVE = ((Sum)ve).getArg();
				if(innerVE instanceof Var)
					variables.add(((Var)innerVE).getName());
				else if(innerVE instanceof MathExpr){
					variables.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
					variables.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
				}
			}
			else if(ve instanceof Avg){
				ValueExpr innerVE = ((Avg)ve).getArg();
				if(innerVE instanceof Var)
					variables.add(((Var)innerVE).getName());
				else if(innerVE instanceof MathExpr){
					variables.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
					variables.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
				}
			}
			else if(ve instanceof Count){
				ValueExpr innerVE = ((Count)ve).getArg();
				if(innerVE instanceof Var)
					variables.add(((Var)innerVE).getName());
				else if(innerVE instanceof MathExpr){
					variables.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
					variables.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
				}
			}
			else if(ve instanceof Min){
				ValueExpr innerVE = ((Min)ve).getArg();
				if(innerVE instanceof Var)
					variables.add(((Var)innerVE).getName());
				else if(innerVE instanceof MathExpr){
					variables.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
					variables.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
				}
			}
			else if(ve instanceof Max){
				ValueExpr innerVE = ((Max)ve).getArg();
				if(innerVE instanceof Var)
					variables.add(((Var)innerVE).getName());
				else if(innerVE instanceof MathExpr){
					variables.add(((Var)((MathExpr)innerVE).getLeftArg()).getName());
					variables.add(((Var)((MathExpr)innerVE).getRightArg()).getName());
				}
			}
			
			for (StatementPattern triple : rootConnectedTriples) {
				if(!triple.getObjectVar().isConstant() && variables.contains(triple.getObjectVar().getName())){
					queryMsrPredicates.add(triple.getPredicateVar().getValue().stringValue());
					queryMsrTriples.add(triple);
				}
			}
			if(variables != null && variables.size() > 0 && queryMsrPredicates.size() > 0 && queryMsrTriples.size() > 0)
				measures.add(new AggregateInfo(aggregateName, new ArrayList<String>(variables), queryMsrPredicates, queryMsrTriples, ee));
		}
		
		return measures;
	}
	
	private static TupleExpr GetSelectExpressionFromView(TupleExpr te){
		TupleExpr retTE = null;
		if(te instanceof UnaryTupleOperator){
			TupleExpr uto = (UnaryTupleOperator)te;
			while (uto instanceof UnaryTupleOperator){
				if(uto instanceof Projection){
					retTE = uto;
				}
				uto = ((UnaryTupleOperator)uto).getArg();
			}
			
		}
		return retTE;
	}
	
	private static String ReplaceQuery(String origQuery, List<String> matViewPatterns, List<StatementPattern> remainingQueryPatterns, //List<StatementPattern>
			String graphURI, List<String> selections, List<String> aggregations, List<String> filters){
		String rewrittenQuery = null;
		String beginning = null; 
		String restQuery = null; 
		int selIndex = origQuery.indexOf("?");
		beginning = origQuery.substring(0,  selIndex);
		restQuery = origQuery.substring(selIndex);
		rewrittenQuery = rewrittenQuery + beginning + " \n";
		
		//// add select and measures
		for (String sel : selections) {
			rewrittenQuery += " ?" + sel;
		}
		for (String agg : aggregations) {
			rewrittenQuery += " " + agg;
		}
		rewrittenQuery += " \n";
		//// don't get select variables, get the rest
		int fromPosition = 0;
		if(restQuery.toUpperCase().contains("FROM")){
			fromPosition = restQuery.toUpperCase().indexOf("FROM");
			restQuery = restQuery.substring(fromPosition);
		} 
		String fromClause = "";
		{
			int wherePosition = restQuery.toUpperCase().indexOf("WHERE");
			if(fromPosition >= 0) fromClause = restQuery.substring(0, wherePosition);
			restQuery = restQuery.substring(wherePosition);
		}
		//// graph pattern
		int startGraphPosition = restQuery.indexOf("{");
		String whereClause = restQuery.substring(0,  startGraphPosition + 1);
		if(fromClause != null && fromClause.length() > 0) 
			rewrittenQuery = rewrittenQuery + fromClause + " \n";
		rewrittenQuery = rewrittenQuery + "FROM NAMED <" + graphURI + "> \n";
		rewrittenQuery = rewrittenQuery + whereClause + " \n";
		int endGraphPosition = restQuery.lastIndexOf("}");
		String endClause = restQuery.substring(endGraphPosition);
		
		//// build MV graph part
		StringBuilder sb = new StringBuilder();
		for (String mvptrn : matViewPatterns) {
			sb.append(mvptrn + "\n");
		}
//		for (StatementPattern sp : matViewPatterns) {
//			////System.out.println("Triple:  ?" + varQueryRoot + " <" + prdct + "> ?" + eeMV.getName() + " ."); 
//			sb.append(((sp.getSubjectVar().getValue() == null) ? ("?" + sp.getSubjectVar().getName()) : ("<" + sp.getSubjectVar().getValue().stringValue() + ">")) + 
//					" <" + sp.getPredicateVar().getValue().stringValue() + "> " + 
//					((sp.getObjectVar().getValue() == null) ? ("?" + sp.getObjectVar().getName()) : ("<" + sp.getObjectVar().getValue().stringValue() + ">")) + " . \n");
//		}		
		rewrittenQuery = rewrittenQuery + "GRAPH <" + graphURI + "> { \n" + sb.toString() + " }\n";
		
		//// build the rest graph part
		sb = new StringBuilder();
		for (StatementPattern sp : remainingQueryPatterns) {
			////System.out.println("Triple:  ?" + varQueryRoot + " <" + prdct + "> ?" + eeMV.getName() + " ."); 
			sb.append(((sp.getSubjectVar().getValue() == null) ? ("?" + sp.getSubjectVar().getName()) : ("<" + sp.getSubjectVar().getValue().stringValue() + ">")) + 
					" <" + sp.getPredicateVar().getValue().stringValue() + "> " + 
					((sp.getObjectVar().getValue() == null) ? ("?" + sp.getObjectVar().getName()) : ("<" + sp.getObjectVar().getValue().stringValue() + ">")) + " . \n");
		}		
		rewrittenQuery = rewrittenQuery + sb.toString() + "\n";
		
		//// add filters
		for (String filt : filters) {
			rewrittenQuery = rewrittenQuery + " FILTER(" + filt + ")" + "\n";
		}
		
		//// end the query
		rewrittenQuery = rewrittenQuery + endClause;
		return rewrittenQuery;
	}

	private static String rewriteQuery(String origQuery, List<String> matViewPatterns, List<StatementPattern> remainingQueryPatterns,  String graphURI, 
			String selections, List<String> filters){
		String rewrittenQuery = "";
		String beginning = null; 
		String restQuery = null; 
		int selIndex = origQuery.toUpperCase().indexOf("SELECT");
		beginning = origQuery.substring(0,  selIndex + 6);
		restQuery = origQuery.substring(selIndex + 6);
		rewrittenQuery = rewrittenQuery + beginning + " \n";
		
		//// add select and measures
		rewrittenQuery += " " + selections + " \n";
		
		//// don't get select variables, get the rest
		int fromPosition = 0;
		if(restQuery.toUpperCase().contains("FROM")){
			fromPosition = restQuery.toUpperCase().indexOf("FROM");
			restQuery = restQuery.substring(fromPosition);
		} 
		String fromClause = "";
		{
			int wherePosition = restQuery.toUpperCase().indexOf("WHERE");
			if(fromPosition > 0) fromClause = restQuery.substring(0, wherePosition);
			restQuery = restQuery.substring(wherePosition);
		}
		//// graph pattern
		int startGraphPosition = restQuery.indexOf("{");
		String whereClause = restQuery.substring(0,  startGraphPosition + 1);
		if(fromClause != null && fromClause.length() > 0) 
			rewrittenQuery = rewrittenQuery + fromClause + " \n";
		else
			rewrittenQuery = rewrittenQuery + " FROM <http://ssb1_qb4o.org>" + " \n";
		rewrittenQuery = rewrittenQuery + "FROM NAMED <" + graphURI + "> \n";
		rewrittenQuery = rewrittenQuery + whereClause + " \n";
		int endGraphPosition = restQuery.lastIndexOf("}");
		String endClause = restQuery.substring(endGraphPosition);
		
		//// build MV graph part
		StringBuilder sb = new StringBuilder();
		for (String mvptrn : matViewPatterns) {
			sb.append(mvptrn + "\n");
		}
//		for (StatementPattern sp : matViewPatterns) {
//			////System.out.println("Triple:  ?" + varQueryRoot + " <" + prdct + "> ?" + eeMV.getName() + " ."); 
//			sb.append(((sp.getSubjectVar().getValue() == null) ? ("?" + sp.getSubjectVar().getName()) : ("<" + sp.getSubjectVar().getValue().stringValue() + ">")) + 
//					" <" + sp.getPredicateVar().getValue().stringValue() + "> " + 
//					((sp.getObjectVar().getValue() == null) ? ("?" + sp.getObjectVar().getName()) : ("<" + sp.getObjectVar().getValue().stringValue() + ">")) + " . \n");
//		}		
		rewrittenQuery = rewrittenQuery + "GRAPH <" + graphURI + "> { \n" + sb.toString() + " }\n";
		
		//// build the rest graph part
		sb = new StringBuilder();
		for (StatementPattern sp : remainingQueryPatterns) {
			////System.out.println("Triple:  ?" + varQueryRoot + " <" + prdct + "> ?" + eeMV.getName() + " ."); 
			sb.append(((sp.getSubjectVar().getValue() == null) ? ("?" + sp.getSubjectVar().getName()) : ("<" + sp.getSubjectVar().getValue().stringValue() + ">")) + 
					" <" + sp.getPredicateVar().getValue().stringValue() + "> " + 
					((sp.getObjectVar().getValue() == null) ? ("?" + sp.getObjectVar().getName()) : ("<" + sp.getObjectVar().getValue().stringValue() + ">")) + " . \n");
		}		
		rewrittenQuery = rewrittenQuery + sb.toString() + "\n";
		
		//// add filters
		for (String filt : filters) {
			rewrittenQuery = rewrittenQuery + " FILTER(" + filt + ")" + "\n";
		}
		
		//// end the query
		rewrittenQuery = rewrittenQuery + endClause;
		return rewrittenQuery;
	}
	
}
