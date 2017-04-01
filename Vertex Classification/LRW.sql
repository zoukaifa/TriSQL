DELIMITER $$
use vertex$$
drop procedure if exists pro$$
create procedure pro(IN nodesize int, IN tmax int, IN sv double, IN r int, In eps double, In tau double)
begin

    # 初始阶段-构造图的转移矩阵，并存储于amatrix
	#call initial(nodesize);
        
    # Explore阶段-创建临时表存储每个种子点对应的特征向量
	drop table if exists tmp;
    create table tmp(
		id int(11) not null auto_increment,
        t int(11) default -1,
        m int(11) default -1,
        f double default 0,
        primary key (id)
    );
            
    # Explore阶段-遍历种子顶点，并计算每个种子顶点的特征向量(0 <= t < Tmax)
    set @xi = 1;
	truncate table nodes;
    while @xi <= nodesize Do
        # 每个种子顶点计算时先清空特征向量表
        truncate table tmp;
        # 初始化种子顶点的特征向量
        set @t = 1;
		set @yi = 1;
		while @yi <= nodesize Do
			begin
				if @yi = @xi then
					insert into tmp(t, m, f) value(@t, @yi, 1);
				else
					insert into tmp(t, m, f) value(@t, @yi, 0);
				end if;
			end;
			set @yi = @yi + 1;
		end while;
                
        # 迭代计算每个种子点的特征向量
		outer_label:begin
        while @t <= tmax Do
            set @rr = 1;
            set @s = 0;

            # 计算t状态下种子点的特征向量
            while @rr <= nodesize Do
				set @ss = 0;
				begin
					select sum(val*f) from amatrix, tmp where colnum = m and rownum = @rr and t = @t into @ss;
                    if @rr=@xi then
						if @ss < sv then
						   set @ss = 0;
						end if;
                        set @ss = power(@ss, r);
					end if;
                    # 计算1范数
                    set @s = @s + @ss;
					insert into tmp(t, m, f) value(@t+1, @rr, @ss);
				end;
                set @rr = @rr + 1;
            end while;
            if @s!=0 then
		        update tmp set f = f/@s where m=@xi and t=@t+1;
			end if;
			set @t = @t + 1;
            
            # 判断是否达到退出条件
			set @epss = 0;
			set @k = 1;
			while @k <= nodesize Do
				set @af = 0;
				set @bf = 0;
				begin
					select f from tmp where t=@t-1 and m=@k into @af;
					select f from tmp where t=@t and m=@k into @bf;
				end;
				set @epss = @epss + power(@bf-@af, 2);
				set @k = @k + 1;
			end while;
            
			if sqrt(@epss) < eps then
				begin
					delete from tmp where t=@t;
				end;
                set @t= @t - 1;
				leave outer_label; 
			end if;
                        
        end while;
        end outer_label;
        
        # 结束迭代后将特征向量保存到nodes表中
        set @k = 1;
        while @k <= nodesize Do
			set @feature = 0.0;
			begin
				select f from tmp where t=@t-1 and m=@k into @feature;
                insert into nodes(idnode, idfeature, val) values(@xi, @k, @feature);
			end;
            set @k = @k + 1;
        end while;
        
		set @xi = @xi + 1;
    end while;
    

	#drop table if exists HashD;
    #create table HashD(
	#	keyname int(11) not null auto_increment,
    #    refval  int(11) default 0,
	# primary key (keyname)
    #);
    #drop table if exists HashV;
    #create table HashV(
	#	refid int(11) not null,
    #    val int(11),
    #    primary key (refid)
    #);
    
    drop table if exists S;
    create table S(
		id int(11) not null unique auto_increment,
        ikey int(11) not null,
        val int(11) not null,
        flag int(11) not null default 1,
        primary key (ikey, val)
    );
    drop table if exists F;
    create table F(
        id int(11) not null unique auto_increment,
		ikey int(11) not null,
        val int(11) not null,
	    primary key (ikey, val)
    );
    
	# Merge阶段-LRW cluster merging phase
	# Merge阶段-创建字典
    truncate table f;
    truncate table s;
    
    set @xi = 1; 
    while @xi <= nodesize Do
		begin
			set @m = 0;
            set @mval = 0;
            set @c = 0;
            # 实现argmax(), 查询特征向量中最大分量对应的下标
			select min(idfeature), val from nodes a where val in 
			(select max(val) as maxval from nodes b where b.idnode=@xi) and a.idnode=@xi into @m, @mval;
            
            # 添加S集合的元素
            insert into s(ikey, val) values(@m, @xi);
            
            # 迭代添加F集合的元素
			begin 
				declare done int default false;
				declare vvv double;
				declare idx int;
				declare fcursor cursor for select idfeature, val from nodes where idnode=@xi;
				declare continue handler for not found set done = true;
				open fcursor;
				myLoop: loop
					fetch fcursor into idx, vvv;
                        if vvv > tau*@mval then
							replace into f(ikey, val) values(@m, idx);
                        end if;
					if done = true then
					leave myLoop;
					end if;
				end loop myLoop;
				close fcursor;
			end;
            
            # 合并字典得到顶点分类结果
            set @ii = 1; 
            while @ii <= 200 Do
				begin
					declare tmp int default -1;
					declare curone int default 0;
					declare curtwo int default 0;
					declare done int default false;
					declare fcursor cursor for select ikey from s group by ikey order by rand() limit 0,2;
					declare continue handler for not found set done = true;
					open fcursor;
					set @i = 1;
					myLoop: loop
						fetch fcursor into tmp;
							if @i = 1 then
								set curone = tmp;
							else
								set curtwo = tmp;
							end if;
							set @i = @i + 1;
						if done = true then
							leave myLoop;
						end if;
					end loop myLoop;
					close fcursor;
					
					set @unum = 0;
					set @anum = 0;
					set @bnum = 0;
					set @minnum = 0;

					select count(*) from vertex.f where ikey=curone into @anum;
					select count(*) from vertex.f where ikey=curtwo into @bnum;
					select count(*) from vertex.f where val in (select val from vertex.f where ikey=curone) and ikey=curtwo into @unum;

					if @anum > @bnum then
						set @minnum = @bnum;
					else
						set @minnum = @abnum;
					end if;
                    
					# 如果满足合并条件则进行合并操作
					if @unum >= 0.5*@minnum then
						# 合并F
						update ignore vertex.f set ikey=curone where ikey = curtwo;
						delete from vertex.f where ikey=curtwo;
						# 合并S
						update ignore s set ikey=curone where ikey=curtwo;
					end if;
                    
				end;
                set @ii = @ii + 1;
            end while;
            # 合并字典结果结束

		set @xi = @xi + 1;
        end;
    end while;
    
end$$

# 输入分类算法参数：nodesize, tmax, sv, r, eps, tau
call pro(34, 100, 0.02, 2, 0.0001, 0.95)$$
DELIMITER ;
	