begin transaction isolation level serializable ;

insert into int_table(id) values (6);

--select * from int_table;
insert into sum(name, count) select '_odds', count(id) from int_table where (id % 2) != 0;

SELECT pg_sleep(1);
commit transaction;
select current_time;
