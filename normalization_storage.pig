/* Filter on the eventname, add the missing user and sort alphabetically the record */

ZZ = LOAD '$input_result' USING PigStorage() AS (eventname:chararray,userid:chararray,normalized_sum:float);
X2 = LOAD '$input_X2' USING PigStorage() AS (unique_userid:chararray);

DEFINE my_macro5(A,X,filter_value) RETURNS SORTED_A {
       A1 = FILTER $A BY (eventname == '$filter_value');
       B = JOIN A1 BY userid RIGHT OUTER, $X BY unique_userid;
       B2 = FOREACH B GENERATE A1::eventname AS eventname, $X::unique_userid AS userid, A1::normalized_sum AS normalized_sum;
       B3 = FOREACH B2 GENERATE (eventname is null ? '$filter_value': eventname) AS eventname, userid AS userid, (normalized_sum is null ? 0 : normalized_sum) AS normalized_sum;
       $SORTED_A = ORDER B3 BY userid;
}

SORTED_LOG = my_macro5(ZZ,X2,'$Log');
Store SORTED_LOG into '$output' using PigStorage();