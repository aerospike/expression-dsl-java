package com.aerospike.dsl;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.index.Index;

import java.util.Collection;

/**
 * Contains API to convert dot separated String path into an Aerospike filter -
 * a functional language for applying predicates to bin data and record metadata.
 * <br>
 * Such filters are used in different areas of Aerospike Server functionality including the following:
 * <ul>
 * <li> filtering queries (acting as the WHERE clause),</li>
 * <li> filtering batch operations,</li>
 * <li> conditionally executing single key operations (get, put, delete, operate),</li>
 * <li> defining secondary indexes.</li>
 * </ul>
 */
public interface DSLParser {

    /**
     * Parse String DSL path into Aerospike filter Expression.
     * <br><br>
     * Examples:
     * <table border="1">
     * <caption>Path element</caption>
     *   <tr>
     *     <td> $.binName </td> <td> Bin “binName” </td>
     *   </tr>
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> '1' </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {='1'} </td>
     *     <td> Map value (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> {#1} </td>
     *     <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     *   </tr>
     * </table>
     * <br>
     * <table border="1">
     * <caption>A nested element</caption>
     *   <tr>
     *     <td> $.mapBinName.k </td> <td> [mapBinName -> mapKey("a")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.aa.aaa </td> <td> [mapBinName -> mapKey("a") -> mapKey("aa") -> mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.55 </td> <td> [mapBinName -> mapKey("a") -> mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> $.listBinName.[1].aa </td> <td> [listBinName -> listIndex(1) -> mapKey("aa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.ab.cd.[-1].'10' </td> <td> [mapBinName -> mapKey("ab") -> mapKey("cd") -> listIndex(-1) ->
     *         mapKey("10")] </td>
     *   </tr>
     * </table>
     *
     * @param input      String consisting of dot separated elements, typically bin name and optional context
     * @return           ParsedExpression object
     * @throws AerospikeDSLException in case of invalid syntax
     */
    ParsedExpression parseDslExpression(String input);

    /**
     * Parse String DSL path into Aerospike filter Expression.
     * <br><br>
     * Examples:
     * <table border="1">
     * <caption>Path element</caption>
     *   <tr>
     *     <td> $.binName </td> <td> Bin “binName” </td>
     *   </tr>
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> '1' </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {='1'} </td>
     *     <td> Map value (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> {#1} </td>
     *     <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     *   </tr>
     * </table>
     * <br>
     * <table border="1">
     * <caption>A nested element</caption>
     *   <tr>
     *     <td> $.mapBinName.k </td> <td> [mapBinName -> mapKey("a")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.aa.aaa </td> <td> [mapBinName -> mapKey("a") -> mapKey("aa") -> mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.55 </td> <td> [mapBinName -> mapKey("a") -> mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> $.listBinName.[1].aa </td> <td> [listBinName -> listIndex(1) -> mapKey("aa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.ab.cd.[-1].'10' </td> <td> [mapBinName -> mapKey("ab") -> mapKey("cd") -> listIndex(-1) ->
     *         mapKey("10")] </td>
     *   </tr>
     * </table>
     *
     * @param input     String consisting of dot separated elements, typically bin name and optional context
     * @param namespace Namespace to use
     * @return ParsedExpression object
     * @throws AerospikeDSLException in case of or invalid syntax
     */
    ParsedExpression parseDslExpression(String input, String namespace, Collection<Index> indexes);

    /**
     * Parse String DSL path into Aerospike filter Expression.
     * <br><br>
     * Examples:
     * <table border="1">
     * <caption>Path element</caption>
     *   <tr>
     *     <td> $.binName </td> <td> Bin “binName” </td>
     *   </tr>
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> '1' </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {='1'} </td>
     *     <td> Map value (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> {#1} </td>
     *     <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     *   </tr>
     * </table>
     * <br>
     * <table border="1">
     * <caption>A nested element</caption>
     *   <tr>
     *     <td> $.mapBinName.k </td> <td> [mapBinName -> mapKey("a")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.aa.aaa </td> <td> [mapBinName -> mapKey("a") -> mapKey("aa") -> mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.55 </td> <td> [mapBinName -> mapKey("a") -> mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> $.listBinName.[1].aa </td> <td> [listBinName -> listIndex(1) -> mapKey("aa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.ab.cd.[-1].'10' </td> <td> [mapBinName -> mapKey("ab") -> mapKey("cd") -> listIndex(-1) ->
     *         mapKey("10")] </td>
     *   </tr>
     * </table>
     *
     * @param input      String consisting of dot separated elements, typically bin name and optional context
     * @return           Expression for filtering records
     * @throws AerospikeDSLException in case of or invalid syntax
     */
    Expression parseFilterExpression(String input);

    /**
     * Parse String DSL path into Aerospike secondary index Filter.
     * <br><br>
     * Examples:
     * <table border="1">
     * <caption>Filter for a bin</caption>
     *   <tr>
     *     <td> $.intBin1 == 10 </td> <td> Filter.equal("intBin1", 10) </td>
     *   </tr>
     *   <tr>
     *     <td> $.intBin1 > 10 </td> <td> Filter.range("intBin1", 11, Long.MAX_VALUE) </td>
     *   </tr>
     *   <tr>
     *     <td> $.stringBin1 == 'text' </td> <td> Filter.equal("stringBin1", "text") </td>
     *   </tr>
     * </table>
     *
     * @param input     String consisting of dot separated elements, typically bin name and value
     * @param namespace Namespace to use
     * @param indexes   List of indexes to choose from. Can be null
     * @return List of Filter objects
     * @throws AerospikeDSLException in case of invalid syntax
     */
    Filter parseFilter(String input, String namespace, Collection<Index> indexes);
}
