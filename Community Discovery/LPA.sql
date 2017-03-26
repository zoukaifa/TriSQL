DELIMITER $$
use GraphEngine$$
set @iteNum = 30$$  # 迭代次数
set @nodeNum = 14$$  # 节点个数
drop procedure if exists pro$$
create procedure pro()
begin
	declare nod int;
    
	# 初始化label表，每个节点标签唯一
	while @nodeNum > 0 do
		insert into label values(14-@nodeNum, 14-@nodeNum);
        insert into temp values(14-@nodeNum, 0, rand());
        set @nodeNum = @nodeNum - 1;
    end while;
	set @nodeNum = 14;  # 恢复nodeNum
    
    while @iteNum > 0 do  # 迭代更新
		set @iteNum = @iteNum - 1;
        while @nodeNum > 0 do  # 逐个节点更新
			update temp set num = 0, ran = rand();  # 首先重置该节点的temp数组
				# 这一块用来调整temp数组
                begin
					declare done int default false;
                    # cur此时为当前节点邻接节点的标签
					declare cur cursor for select label.label from graph 
						inner join label on graph.node2 = label.node
							where graph.node1 = 14-@nodeNum;
					declare continue handler for not found set done = true;
                    
                    open cur;
                    fetchLoop: loop
						fetch cur into nod;
						if done then
							leave fetchLoop;
						else
							update temp set num = num + 1 where label = nod; 
						end if;
					end loop;
                    close cur;
                end;
                # 接下来选取temp中，num最大的label，并更新
                select label from temp order by num desc, ran desc limit 1 into nod;
                update label set label = nod where node = 14-@nodeNum;
            
			set @nodeNum = @nodeNum - 1;
		end while;
	end while;
    
    select * from label;
	delete from label;
	delete from temp;
end$$
call pro()$$
DELIMITER ;