
GH = LOAD '$input' USING PigStorage() AS (acctid:chararray,userid:chararray,eventname:chararray,usage:float,periodkey:chararray);


/* Finds min_pk by userid over all eventname for GH dataset */
GH_date = FOREACH GH GENERATE ToDate(periodkey,'yyyy-MM-dd HH') as pk, acctid AS acctid, userid AS userid, eventname AS eventname, usage AS usage;

GH_group = GROUP GH_date BY userid;

GH_minpk = FOREACH GH_group {
	ordered_date = ORDER GH_date BY pk;
	min_date = LIMIT ordered_date 1;
	GENERATE group AS userid, FLATTEN(min_date.pk) AS minpk_userid;
}

/* for each eventname find the min_usage and max_usage and normalize that feature */
GH_group_eventname = GROUP GH_date BY eventname;


GH_group_minmax = FOREACH GH_group_eventname{
		distinct_eventname = DISTINCT GH_date.eventname;
		 GENERATE FLATTEN(distinct_eventname) AS eventname, MIN(GH_date.usage) AS event_min, MAX(GH_date.usage) AS event_max;
}

Store GH_group_minmax into '$intermediate' using PigStorage();

U2 = FOREACH GH_date GENERATE eventname AS eventname ,usage AS usage,userid AS userid, pk AS pk;
X = JOIN GH_group_minmax BY eventname, U2  BY eventname;

Y = FOREACH X GENERATE GH_group_minmax::eventname AS eventname, GH_group_minmax::event_min AS event_min, GH_group_minmax::event_max AS event_max, FLATTEN(U2::usage) AS usage,U2::userid AS userid, U2::pk AS pk;

num_denom = FOREACH Y GENERATE eventname AS eventname, userid AS userid,pk AS pk, (usage-event_min) AS num, (event_max-event_min) AS denom;

norm = FOREACH num_denom GENERATE eventname AS eventname,userid AS userid,pk AS pk, (denom != (float)0 ? num/denom:(float)0) AS normalized_usage;

W = JOIN norm BY userid, GH_minpk BY userid;

W2 = FOREACH W GENERATE norm::eventname AS eventname, norm::userid AS userid, norm::pk AS pk, norm::normalized_usage AS normalized_usage, GH_minpk::minpk_userid AS minpk_userid;



Z2 = FILTER W2 BY DaysBetween((datetime)pk,(datetime)minpk_userid) <=(long)$Nb_days;
Z = GROUP Z2 BY (eventname,userid);
Z3 = FOREACH Z GENERATE group.eventname AS eventname, group.userid AS userid, Z2.normalized_usage AS normalized_usage;

ZZ = FOREACH Z3 GENERATE eventname AS eventname ,userid AS userid , SUM(normalized_usage) AS normalized_sum;

X = GROUP ZZ BY userid;
X2 = FOREACH X {
   unique_userid = DISTINCT ZZ.userid;
   GENERATE FLATTEN(unique_userid) AS unique_userid;
}

Store ZZ into '$output_result' using PigStorage();
Store X2 into '$output_X2' using PigStorage();

