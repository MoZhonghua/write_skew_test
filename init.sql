create table if not exists int_table (id int primary key);
create table if not exists sum (name varchar primary key, count int);

delete from int_table;
insert into int_table(id) values(0);
insert into int_table(id) values(2);
insert into int_table(id) values(4);
delete from sum;

select * from int_table;
