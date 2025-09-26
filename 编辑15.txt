10.104.2.160



id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        pwrt float comment '功率kw*h'
    ) comment='风机功率表';
        
id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wind_bin float comment '风速m/s',
        pwrt_mean float comment '多台风机平均功率kw*h',
        pwrt float comment '功率kw*h',
        count float comment '风仓统计频数'
        
        
id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wind_bin float comment '风速m/s',
        pwrt_mean float comment '多台风机平均功率kw*h',
        pwrt float comment '功率kw*h',
        count float comment '风仓统计频数'
    ) comment='风机功率曲线表';
    
    
    
    
    
create_technology_loss_all_table_query = f'''
    create table technology_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        fault float comment '故障标识',
        count float comment '故障频数',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率',
        fault_describe text comment '故障描述',
    ) comment='技术故障损失';
'''
create_limturbine_loss_all_table_query = f'''
    create table limturbine_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率'
    ) comment='单机限电损失';
'''
create_faultgrid_loss_all_table_query = f'''
    create table faultgrid_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率',
        count float comment '故障次数',
        fault float comment '故障标识',
        fault_describe text comment '故障描述'
    ) comment='电网故障损失';
'''
create_stop_loss_all_table_query = f'''
    create table stop_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        exltmp float comment '环境温度',
        time_rate float comment '时间利用率',
        loss float comment '损失功率'
    ) comment='计划停机损失';
'''
create_limgrid_loss_all_table_query = f'''
    create table limgrid_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率'
    ) comment='电网限电损失';
'''
create_fault_loss_all_table_query = f'''
    create table fault_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率',
        count float comment '故障次数',
        fault float comment '故障标识',
        fault_describe text comment '故障描述',
        fsyst text comment '系统故障'
    ) comment='单机故障损失';
'''


docker run -d --name mysql -p 13306:3306 -e MYSQL_ROOT_PASSWORD=Zhang123. -v ~/docker/mysql/data:/var/lib/mysql --restart=always -d mysql --character-set-server=utf8mb4 --collation-server=utf8mb4_general_ci
docker run -d -p 19000:9000 -p 19090:9090 --name minio  -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=Zhang123." -v ~/docker/minio:/data minio/minio server /data --console-address ":9090"


rWlbaEnM


"INSERT INTO wind_direction_picture (excute_time,farm_name,farm_id, \type_name, \wtid, \minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"


                #nan验证
                if tmp.iloc[j]['windbin'] == np.nan:
                    windbin = nan
                else:
                    windbin = tmp.iloc[j]['windbin']
                if tmp.iloc[j]['pwrat'] == np.nan:
                    pwrat = nan
                else:
                    pwrat = tmp.iloc[j]['pwrat']
                if tmp.iloc[j][turbine_name+'_count'] == np.nan:
                    count = nan
                else:
                    count = tmp.iloc[j][turbine_name+'_count']
                if tmp.iloc[j][turbine_name+'_wspd'] == np.nan:
                    wspd = nan
                else:
                    wspd = tmp.iloc[j][turbine_name+'_wspd']
                if tmp.iloc[j][turbine_name] == np.nan:
                    pwrat = nan
                else:
                    pwrat = tmp.iloc[j][turbine_name]
                if tmp.iloc[j]['count'] == np.nan:
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan:
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan:
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan:
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            if tmp.iloc[j]['exltmp'] == np.nan:
                exltmp = nan
            else:
                exltmp = tmp.iloc[j]['exltmp']   
                
                
                        #nan验证
            if lineValue[5] == nan:
                lineValue[5] = np.nan
            if lineValue[6] == nan:
                lineValue[6] = np.nan
            if lineValue[7] == nan:
                lineValue[7] = np.nan
            if lineValue[8] == nan:
                lineValue[8] = np.nan
                
                
                
                
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
                
            log.info(f'sql语句：{queryItem}')
            log.info(f'sql数据：{dataQuery}')
            
            log.info(f'sql语句：{check_table_query}')
            
            log.info(f'sql语句：{}')
            
            
            
            # 创建多层列名
df.columns = pd.MultiIndex.from_tuples([
    ('Group1', 'A'),
    ('Group1', 'B'),
    ('Group2', 'C'),
    ('Group2', 'D')
])

print("\n修改后的DataFrame:")
print(df)



机组故障
{
	"table":[
		{
			"wtid":"#06",
			"faultCode": "",
			
		}
	]
}