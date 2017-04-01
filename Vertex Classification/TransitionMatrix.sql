DELIMITER $$
use vertex$$
drop procedure if exists initial$$
# 计算概率转移矩阵
create procedure initial(IN nodesize INT)
begin
    # 创建及初始化邻接矩阵A
	drop table if exists amatrix;
    create table amatrix(
		id int(11) not null auto_increment,
        val double default 0,
        rownum int(11) default -1,
        colnum int(11) default -1,
        primary key (id)
    );
    
	set @xi = 1;
	#truncate table amatrix;
	while @xi <= nodesize*nodesize Do
		begin
			if @xi%nodesize = 0 then 	
				insert into amatrix(val, rownum, colnum) value(0.0, (@xi-@xi%nodesize)/nodesize, nodesize);
			else
				insert into amatrix(val, rownum, colnum) value(0.0, (@xi-@xi%nodesize)/nodesize + 1, @xi%nodesize);
			end if;
			set @xi = @xi + 1;
		end;
	end while;
    
	begin 
	    declare edgeid int default 0;
        declare snode int default 0;
        declare tnode int default 0;
        declare done int default false;
		declare myCursor cursor for select idedges, source, target from edges;
		declare continue handler for not found set done = true;
		open myCursor;
		myLoop: loop
			fetch myCursor into edgeid, snode, tnode;
				update amatrix set val=1 where id=(snode-1)*nodesize+tnode or id=(tnode-1)*nodesize+snode;
            if done = true then
            leave myLoop;
            end if;
		end loop myLoop;
        close myCursor;
    end;
        
    # 创建及初始化对角矩阵D
    drop table if exists dmatrix;
    create table dmatrix(
		id int(11) not null auto_increment,
        val double default 0,
        primary key (id)
    );
    
    set @xi = 1;
    set @colsum = 0;
    set @tmp = 0;
   # truncate table dmatrix;
    while @xi <= nodesize Do
		begin
			select sum(val) from amatrix where colnum=@xi into @tmp;
            set @colsum = @colsum + @tmp;
			set @xi = @xi + 1;
		end;
        begin
			insert into dmatrix(val) value(@colsum);
            set @colsum = 0;
		end;
	end while;
    
    # 利用公式计算转移概率矩阵P = (I + A)(I + D)-1
    set @xi = 1;
    while @xi <= nodesize Do 
		begin
			update amatrix set val = val + 1 where id = @xi*(nodesize+1) - nodesize;
        end;
        begin 
			update dmatrix set val = 1/(val + 1) where id = @xi;
        end;
        set @xi = @xi + 1;
	end while;
    
    set @xi = 1;
    set @bi = -1;
    while @xi <= nodesize Do
		begin
			select val from dmatrix where id = @xi into @bi;
        end;
        begin
			update amatrix set val = val*@bi where colnum = @xi;
        end;
        set @xi = @xi + 1;
	end while;
    # 计算完毕后amatrix表中存储的就是转移矩阵
    
end$$
call initial(34)$$
DELIMITER ;