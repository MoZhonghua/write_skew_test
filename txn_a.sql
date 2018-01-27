begin transaction isolation level serializable ;

insert into int_table(id) values (1);
--update int_table set id = 1 where id = 1;
--insert into int_table(id) values (1);

--select * from int_table;
--insert into sum(name, count) select 'total', count(id) from int_table;
insert into sum(name, count) select '_evens', count(id) from int_table where (id % 2) = 0;

SELECT pg_sleep(3);
commit transaction;
select current_time;
