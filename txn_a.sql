begin transaction isolation level serializable ;

insert into int_table(id) values (1);

insert into sum(name, count) select '_evens', count(id) from int_table where (id % 2) = 0;

SELECT pg_sleep(5);
commit transaction;
