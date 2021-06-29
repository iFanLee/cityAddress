package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

/**
 * @Author: Lee
 * @Date: 2021/6/29 9:45
 * @Desc:从百度地图行政区划adcode映射表（https://lbsyun.baidu.com/index.php?title=open/dev-res）解析成MySQL四级（省、市、县、镇）城市地址。
		 下载excel后，转成csv，重命名为Township_Area.csv
 */
type cityItem struct {
	Id int32
	Pid int32
	Name string
}
func main()  {
	city := make(map[string]map[string]map[string][]string)
	line,err := readFileByLine("./Township_Area.csv")
	if err!=nil{
		fmt.Println("err:",err)
		return
	}
	//1.把数据转换成四级map结构
	for _,v := range line{
		if strings.Contains(v,"苏鲁交界")||strings.HasPrefix(v,"name_prov"){
			continue
		}
		tmpArr := strings.Split(v,",")
		if _,ok := city[tmpArr[0]];!ok{
			city[tmpArr[0]] = make(map[string]map[string][]string)
		}
		if _,ok := city[tmpArr[0]][tmpArr[2]];!ok{
			city[tmpArr[0]][tmpArr[2]] = make(map[string][]string)
		}
		if _,ok := city[tmpArr[0]][tmpArr[2]][tmpArr[4]];!ok{
			city[tmpArr[0]][tmpArr[2]][tmpArr[4]] = make([]string,0)
		}
		city[tmpArr[0]][tmpArr[2]][tmpArr[4]] = append(city[tmpArr[0]][tmpArr[2]][tmpArr[4]],tmpArr[6])
	}

	//省份数量
	proNum := int32(0)
	//城市数量
	cityNum := int32(0)
	//县城数量
	areaNum := int32(0)
	//镇数量
	townNum := int32(0)

	//计算数量
	for _,v := range city{
		proNum++
		for _,vv := range v{
			cityNum++
			for _,vvv := range vv{
				areaNum++
				for range vvv{
					townNum++
				}
			}
		}
	}


	//省份ID 起始值
	proId := int32(1)
	//城市ID 起始值
	cityId := proId+proNum
	//县城ID 起始值
	areaId := cityId+cityNum
	//镇ID 起始值
	townId := areaId+areaNum


	//要插入数据数组
	sqlArr := make([]cityItem,0)
	for k,v := range city{
		//省份
		sqlArr = append(sqlArr,cityItem{
			Id:   proId,
			Pid: 0,
			Name: k,
		})
		//城市
		for kk,vv := range v{
			sqlArr = append(sqlArr,cityItem{
				Id:   cityId,
				Pid: proId,
				Name: kk,
			})
			//县
			for kkk,vvv := range vv{
				sqlArr = append(sqlArr,cityItem{
					Id:   areaId,
					Pid: cityId,
					Name: kkk,
				})
				//镇
				for _,vvvv := range vvv{
					sqlArr = append(sqlArr,cityItem{
						Id:   townId,
						Pid: areaId,
						Name: vvvv,
					})
					//镇
					townId ++
				}
				areaId ++
			}
			cityId ++
		}
		proId++
	}

	//构建插入MySQL语句
	/*
	CREATE TABLE `dx_city` (
	  `id` int(5) unsigned NOT NULL AUTO_INCREMENT,
	  `pid` int(5) unsigned NOT NULL DEFAULT '0',
	  `name` varchar(30) NOT NULL DEFAULT '',
	  `create_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	  `update_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `unq_pid_name` (`pid`,`name`) USING BTREE COMMENT '同一级别不同名'
	) ENGINE=InnoDB AUTO_INCREMENT=43670 DEFAULT CHARSET=utf8mb4
	*/

	sql := "INSERT INTO dx_city(id,pid,`name`) VALUES"
	count := 1
	for _,v := range sqlArr{
		if count==1{
			sql += fmt.Sprintf("(%v,%v,'%v')",v.Id,v.Pid,v.Name)
		}else {
			sql += fmt.Sprintf(",(%v,%v,'%v')",v.Id,v.Pid,v.Name)
		}
		//为了防止一个SQL语句过大，可以分割成一定数量插入一次
		if count>999{
			mysqlExec(sql)
			sql = "INSERT INTO dx_city(id,pid,name) VALUES"
			count = 1
		}else {
			count++
		}
	}
	//最后判断是否还有未插入的数据，有则执行插入
	if !strings.HasSuffix(sql,"VALUES"){
		mysqlExec(sql)
	}
}
//这里是执行SQL语句，可自定义
func mysqlExec(sql string)  {

}


//按行读取文件内容
func readFileByLine(filePath string) ([]string,error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil,err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	ret := make([]string,0)
	for scanner.Scan() {
		line := scanner.Text()
		ret = append(ret,line)
	}
	if err := scanner.Err(); err != nil {
		return nil,err
	}
	return ret,nil
}