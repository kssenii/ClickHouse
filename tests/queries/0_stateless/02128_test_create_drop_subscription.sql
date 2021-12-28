drop table if exists subscriber;
drop table if exists src;
drop stream if exists s;

create table subscriber (time DateTime, cnt Int32) engine=Memory;
create table src (a Int32, b Int32, c String) engine=Memory;

create stream s as select now(), count() from src;

create subscription from subscriber to s;
show subscriptions from s;

insert into src select number, toString(number), toString(number) from numbers(10);
select * from subscriber;

drop subscription from subscriber to s;
drop table subscriber;
drop stream s NO DELAY;
drop table src;
