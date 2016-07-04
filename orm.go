package orm

/*

	one2many:格式为one2many(关联表，字段) 表示该字段存储所有关联表对应字段为本Model的Id值的记录
	many2one:格式many2one(关联表) 用于外键关系，表示该字段对应关联表里的某个记录
	many2many:many2many(关联表，关联多对多表，该Model的字段，管理表字段)多对多一般关系存储于xxx_rel表里对应2个字段
*/
import (
	"database/sql"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	//	"time"
	"encoding/json"
	"vectors/utils"

	core "github.com/go-xorm/core"
	orm "github.com/go-xorm/xorm"
	//_ "github.com/lib/pq"
)

type (
	TExecResult int64

	TTable struct {
		_cls_type     reflect.Type
		Name          string
		Fields        map[string]*TField
		RecordField   *TField // 表的唯一主键字段 自增/主键/唯一 如：Id
		InheritFields map[string]*TRelateField
		Inherits      []string          //Pg数据库表继承
		Relations     map[string]string // many2many many2one... 等关联表
		RelateFields  map[string]*TRelateField
	}

	TOrm struct {
		*orm.Engine

		TagIdentifier string // tag 标记
		Tables        map[reflect.Type]*TTable

		nameIndex map[string]*TTable
		//DBName  string      // 绑定的数据库名称
		//DBRead  *orm.Engine // 读写分离
		//DBWrite *orm.Engine // 读写分离
	}

	TOrmSession struct {
		*orm.Session
		Orm *TOrm
	}
)

var (
	DbType       string = "postgres"
	DbUser       string = "postgres"
	DbPassword   string = "postgres"
	DbMasterHost string = "localhost:5432"
	DbSlaveHost  string = "localhost:5432"
	// pg:only "require" (default), "verify-full", and "disable" supported
	DbSSLMode   string = "disable" // 字符串
	TestShowSql bool   = true
)

func NewOrm(db, host string) (res *TOrm, err error) {
	res = &TOrm{
		Tables:    make(map[reflect.Type]*TTable),
		nameIndex: make(map[string]*TTable),
	}

	lCnnstr := ""

	switch DbType {
	case "mysql":
		if DbMasterHost[0] == '/' { // looks like a unix socket
			lCnnstr = fmt.Sprintf("%s:%s@unix(%s)/%s?charset=utf8&parseTime=true",
				DbUser, DbPassword, host, db)
		} else {
			lCnnstr = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true",
				DbUser, DbPassword, host, db)
		}
	case "postgres":
		lCnnstr = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
			url.QueryEscape(DbUser), url.QueryEscape(DbPassword), host, db, DbSSLMode)
	default:
		return nil, fmt.Errorf("Unknown database type: %s", DbType)
	}

	//数据库链接
	res.Engine, err = orm.NewEngine(DbType, lCnnstr)
	res.Engine.TagIdentifier = "field"
	if !utils.LogErr(err) {
		//控制台打印SQL语句
		res.Engine.ShowSQL(TestShowSql)

		//控制台打印调试信息
		//res.Engine.ShowDebug = true

		//控制台打印错误信息
		//res.Engine.ShowErr = true

		//控制台打印警告信息
		//res.Engine.ShowWarn = false

	}
	/*
		// Mapping orm
		var table *TTable
		var lName string
		tables, err := res.Engine.DBMetas()
		for _, tb := range tables {
			utils.Logger.DebugLn("SyncModels Fields ", tb,tb.Name, tb.Type)

		}
		for k, tb := range res.Engine.Sync()Tables {
			lName = utils.SnakeCasedName(utils.Obj2Name(k))
			table = NewTable(lName, k)
			// 遍历获得字段
			for _, col := range tb.Columns() {
				lField := NewField()
				lField.name = col.Name
				lField._type = FieldTypes[col.SQLType.Name]
				//utils.Logger.DebugLn("col.SQLType.Name Fields ", col.SQLType.Name)
				table.Fields[col.Name] = lField
				utils.Logger.DebugLn("SyncModels Fields ", k, table.Fields)
			}

			res.Tables[k] = table
		}
	*/
	return
}

func NewTable(name string, t reflect.Type) *TTable {
	return &TTable{
		_cls_type:     t,
		Name:          name,
		Fields:        make(map[string]*TField),
		InheritFields: make(map[string]*TRelateField), // 废弃
		Inherits:      make([]string, 0),              //by hzm
		Relations:     make(map[string]string),
		RelateFields:  make(map[string]*TRelateField),
	}
}

func (v TExecResult) LastInsertId() (int64, error) {
	return int64(v), nil
}

func (v TExecResult) RowsAffected() (int64, error) {
	return int64(v), nil
}

func (self *TOrm) TableByName(name string) *TTable {
	return self.nameIndex[name]
}

func (self *TOrm) TableByType(t reflect.Type) *TTable {
	return self.Tables[t]
}

// TODO 更新表信息
func (self *TOrm) _updateTable(table string) {
	//for self.Engine.Dialect().GetColumns()
}

// New a session
func (self *TOrm) NewSession() *TOrmSession {
	session := &TOrmSession{
		Orm:     self,
		Session: &orm.Session{Engine: self.Engine},
	}
	session.Init()
	return session
}

func splitTag(tag string) (tags []string) {
	tag = strings.TrimSpace(tag)
	var hasQuote = false
	var lastIdx = 0
	for i, t := range tag {
		if t == '(' || t == ')' { // t == '\''
			hasQuote = !hasQuote
		} else if t == ' ' {
			if lastIdx < i && !hasQuote {
				tags = append(tags, strings.TrimSpace(tag[lastIdx:i]))
				lastIdx = i + 1
			}
		}
	}
	if lastIdx < len(tag) {
		tags = append(tags, strings.TrimSpace(tag[lastIdx:len(tag)]))
	}
	return
}

func parseTag(tag string) (tags []string) {
	tag = strings.TrimSpace(tag)
	var (
		hasQuote          = false
		hasSquareBrackets = false
		lastIdx           = 0
		l                 = len(tag)
	)
	for i, t := range tag {
		//if t == '\'' {
		//	hasQuote = !hasQuote
		//} else
		//fmt.Println(t,i)
		if t == '[' || t == ']' {
			hasSquareBrackets = !hasSquareBrackets
		} else if t == '(' || t == ',' || t == ')' { //处理 Tag(xxx)类型
			if lastIdx < i && !hasQuote && !hasSquareBrackets {
				tags = append(tags, strings.TrimSpace(tag[lastIdx:i]))
				lastIdx = i + 1
			}
		} else if i+1 == l { // 处理无括号类型的Tag
			tags = append(tags, strings.TrimSpace(tag[lastIdx:l]))
		}

	}
	//if lastIdx < len(tag) {
	//	tags = append(tags, strings.TrimSpace(tag[lastIdx:len(tag)]))
	//}
	return
}

func (self *TOrm) tag_related(tbl *TTable, fld *TField) {
	// 遍历获得原始字段 例如：Extends其他表的字段
	/*	if fld.related && fld.relmodel_name != "" {

			if table, has := self.TableByName(fld.relmodel_name); !has {
				self.SyncModel(reflect.Zero(table._cls_type).Interface())
				utils.Dbg("tag_related", lFieldType)
			}
			// 添加Extend字段
			lFieldTable := self.Tables[lFieldType]
			if lFieldTable == nil {
				utils.Logger.Error("extends failed!")
			}
			for _, fld := range lFieldTable.Fields {
				//d待续

				if _, has := lTable.Fields[fld.Name]; !has {
					//lNewFld := fld.New()
					//lNewFld._inherit = true
					//lNewFld.cokey_field_name=
					//lNewFld.relkey_field_name=
					lTable.Fields[fld.Name] = fld
					utils.Dbg("5999fld", fld.Name)

					if fld.primary_key && fld.auto_increment {
						lTable.RecordField = fld
						utils.Dbg("RecordField", fld.Name)
					}

					// 记录继承字段
					lModelName := utils.TitleCasedName(utils.DotCasedName(lFieldTable.Name))
					lTable.InheritFields[fld.Name] = NewInheritField(fld.Name, lModelName, "", fld, nil)
				}
			}
		}
	*/
}

func (self *TOrm) tag_one2many(fld *TField, arg ...string) { //comodel_name string, inverse_name string
	fld.read = false
	fld.write = false
	if len(arg) > 1 {
		fld.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0])) //目标表
		fld.cokey_field_name = utils.SnakeCasedName(arg[1])                 //目标表关键字段

		fld.Relation = fld.comodel_name
		fld._type = "one2many" //TODO 剔除掉
		fld.Type = "one2many"
	} else {
		utils.Panic("One2Many field " + fld.Name + "'s Args can no be blank!")
	}
}

func (self *TOrm) tag_many2one(fld *TField, arg ...string) { //comodel_name string
	fld.read = false
	fld.write = true
	if len(arg) > 0 {
		fld.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0])) //目标表
		fld.Relation = fld.comodel_name
		fld._type = "many2one" //TODO 剔除掉
		fld.Type = "many2one"
	} else {
		utils.Panic("Many2One field ", fld.Name, "'s Args can no be blank!")
	}

}

func (self *TOrm) tag_many2many(fld *TField, arg ...string) { //comodel_name, relation, key_field1, key_field2 string
	fld.read = false
	fld.write = false
	if len(arg) > 3 {
		fld.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0]))  //目标表
		fld.relmodel_name = utils.DotCasedName(utils.TitleCasedName(arg[1])) //提供目标表格关系的表
		fld.cokey_field_name = utils.SnakeCasedName(arg[2])                  //目标表关键字段
		fld.relkey_field_name = utils.SnakeCasedName(arg[3])                 // 关系表关键字段
		fld.Relation = fld.comodel_name
		fld._type = "many2many" //TODO 剔除掉
		fld.Type = "many2many"
	} else {
		utils.Panic("Many2Many field ", fld.Name, "'s Args can no be blank!")
	}

}

// TODO 方法可以是任何大小写 参考https://github.com/alangpierce/go-forceexport
// 所有的selection 函数必须是大写并返回map[string]interface{}格式
func (self *TOrm) tag_selection(modelType reflect.Value, fld *TField, arg ...string) { //comodel_name, relation, key_field1, key_field2 string
	if len(arg) > 0 {
		lStr := strings.Trim(arg[0], "'")
		lStr = strings.Replace(lStr, "''", "'", -1)
		if m := modelType.MethodByName(lStr); m.IsValid() {
			results := m.Call([]reflect.Value{modelType}) //
			if len(results) == 1 {
				fld.Selection, _ = results[0].Interface().(map[string]interface{})
			}
		} else {
			err := json.Unmarshal([]byte(lStr), &fld.Selection)
			utils.LogErr(err)
		}
		fld._type = "selection" //TODO 剔除掉
		fld.Type = "selection"
	} else {
		utils.Panic("selection field ", fld.Name, "'s Args can no be blank!")
	}
}

func (self *TOrm) tag_function(fld *TField, arg ...string) {

}

// 重新映射Model 获取字段信息
// TODO 优化速度逻辑

//func (self *TOrm) mapType(v reflect.Value, t *core.Table) *core.Table {
func (self *TOrm) mapType(model interface{}, t *core.Table) (lTable *TTable, lOrgTable *core.Table) {
	v := reflect.Indirect(reflect.ValueOf(model))
	lType := v.Type()

	// 确认信ORM包含该表 以便更新
	lTable = self.Tables[lType]
	if lTable == nil {
		lTable = NewTable(t.Name, lType)
	}

	// 创建一个原始ORM表
	lOrgTable = core.NewTable(t.Name, t.Type)

	var (
		lField                  *TField
		lCol                    *core.Column
		lMemberName, lFieldName string
		has                     bool
	)

	//>>>>>>>>>
	//# Xorm Bug Fix
	// type User struct {
	//    Id   int64
	//    Name string  `xorm:"varchar(25) notnull unique 'usr_name'"`   //这里的usr_name 如果是乱码XORM也将设置Col名为该乱码
	//}
	//而且core.Table不支持FieldName查询和Col的删除。
	//Fix:取出所有Col并重新修改Name再临时保存到Map中。重新创建一个core.Table用于重组并作为函数的结果返回
	lColMap := make(map[string]*core.Column)
	for _, lCol = range t.Columns() {
		//utils.Dbg("t:", lCol.Name, lCol.FieldName, utils.SnakeCasedName(lCol.FieldName))
		lCol.Name = utils.SnakeCasedName(lCol.FieldName)
		lColMap[utils.SnakeCasedName(lCol.FieldName)] = lCol
	}
	//<<<<<<<<<
	lRelateFields := make([]string, 0)
	for i := 0; i < lType.NumField(); i++ {
		lMemberName = lType.Field(i).Name
		lFieldName = utils.SnakeCasedName(lMemberName)
		lFieldValue := v.Field(i)
		//		lFieldType := lType.Field(i).Type
		lFieldTag := lType.Field(i).Tag

		// 忽略无Tag的匿名继承结构
		//utils.Dbg(lType.Field(i).Name, lFieldType.Name(), lTag)
		if lType.Field(i).Name == lType.Field(i).Type.Name() && lFieldTag == "" {
			continue
		}

		if lCol, has = lColMap[lFieldName]; !has {
			lCol = core.NewColumn(lFieldName, lFieldName, core.SQLType{"", 0, 0}, 0, 0, true)
		}

		// 如果 Field 不存在于ORM中
		if lField, has = lTable.Fields[lFieldName]; !has && (lField == nil) {
			lField = NewField()
		} else {
			//<** 如果是继承的字段则替换
			//原因：Join时导致Select到的字段为关联表字段而获取不到原本Model的字段如Id,write_time...
			if lField.foreign_field {
				lField = NewField()
			}
		}

		// TODO 实现继承表 Inherite
		// TODO 自己映射脱离第三方ORM

		// 默认赋值
		lField.Name = lFieldName
		lField._symbol_c = `%s`         // # 修改字段格式化
		lField._symbol_f = _FieldFormat //

		if lCol != nil {
			//utils.Dbg("lCol != nil", lCol.SQLType.Name, FieldTypes[lCol.SQLType.Name])
			lField._type = FieldTypes[lCol.SQLType.Name]
			lField.Type = FieldTypes[lCol.SQLType.Name]

			// 修正三方ORM字段类型格式
			switch FieldTypes[lCol.SQLType.Name] {
			case "char":
				lField._symbol_c = `'%s'` // # 修改字段格式化
			}
		}

		//lField.String = lTag.Get("string")
		//lField.Help = lTag.Get("help")
		//utils.Dbg("mapType", lFieldName, lTag, lTag.Get("string"), lTag.Get("help"))

		// 解析并变更默认值
		//utils.Dbg("ccc", lFieldName, lCol, lFieldTag)
		var (
			lTag    []string
			lStr    string
			lLen    int
			lIgonre bool
		)
		lTags := splitTag(lFieldTag.Get("field"))
		for _, key := range lTags {
			//>>>>>>>>>
			//以下代码是为了避免XORM解析不规则字符串为字段名提醒使用者规范Tag数据格式应该注意不用空格
			lTag = parseTag(key)
			lStr = strings.Replace(key, lTag[0], "", 1) // 去掉Tag Item
			lStr = strings.TrimLeft(lStr, "(")
			lStr = strings.TrimRight(lStr, ")")
			lLen = len(lStr)
			if lLen > 0 {
				if strings.Index(lStr, " ") != -1 {
					if !strings.HasPrefix(lStr, "'") &&
						!strings.HasSuffix(lStr, "'") {
						utils.Logger.Error("Model %s's %s tags could no including space ' ' in brackets value whicth it not 'String' type.", t.Name, strings.ToUpper(lFieldName))
						panic("")
					}
				}
			}
			lIgonre = false
			//<<<<<<<<<<<
			//utils.Dbg("tag:", key, lTag)
			// 原始ORM映射,理论上无需再次解析只需修改Tag和扩展后的一致即可
			switch strings.ToLower(lTag[0]) {
			case "-": // 忽略某些继承者成员
				lIgonre = true
				break
			case "--": // 只映射不修改进数据库
				break
			case "extends", "relate": // 忽略某些继承者成员
				is_relate := strings.ToLower(lTag[0]) == "relate"

				if is_relate {
					if len(lTag) > 1 {
						lRelFldName := utils.SnakeCasedName(lTag[1])
						lRelateFields = append(lRelateFields, lRelFldName)
						utils.Dbg("relate to:", utils.DotCasedName(lMemberName), lRelFldName)
						// 现在成员名是关联的Model名,Tag 为关联的字段
						lTable.Relations[utils.DotCasedName(lMemberName)] = lRelFldName
					}
				} else {
					//  extends
				}

				switch lFieldValue.Kind() {
				case reflect.Ptr:
					utils.Logger.Error("field:%s as pointer is not supported!", lFieldName)
					break
				case reflect.Struct:
					var (
						newParentTable *TTable // 新ORM的Table
						parentTable    *core.Table
					)

					parentTable = self.TableInfo(lFieldValue.Interface())
					newParentTable, parentTable = self.mapType(lFieldValue.Interface(), parentTable)
					//utils.Dbg("parent", parentTable, parentTable.Name, lFieldValue.Interface())
					//var lNewFld *TField
					for _, fld := range newParentTable.Fields {
						//d待续
						if _, has := lTable.Fields[fld.Name]; !has {
							//lNewFld = new(TField)
							lNewFld := *fld //复制关联字段
							utils.Dbg("FFF", newParentTable.Name, fld, fld.Name, lNewFld)

							if is_relate {
								lNewFld.foreign_field = true
							}

							//lNewFld := fld.New()
							//lNewFld._inherit = true
							//lNewFld.cokey_field_name=
							//lNewFld.relkey_field_name=
							//utils.Dbg("5999fld", fld.Name)

							//# 以下因为使用postgres 的继承方法时Model部分字段是由Parent继承来的
							//# 映射时是没有Parent的字段如Id 所以在此获取Id主键.
							if lNewFld.primary_key && lNewFld.auto_increment {
								lTable.RecordField = &lNewFld
								utils.Dbg("RecordField", fld.Name)
							}

							if _, has := lTable.Fields[fld.Name]; !has {
								lTable.Fields[fld.Name] = &lNewFld
							}

							// 记录继承字段
							//lModelName := utils.TitleCasedName(utils.DotCasedName(lFieldTable.Name))
							//lTable.InheritFields[fld.Name] = NewRelateField(fld.Name, lModelName, "", fld, "")
						}
					}
				}
				/*   V1 废弃
				// 遍历获得原始字段 例如：Extends其他表的字段
				if _, has := self.Engine.Tables[lFieldType]; !has {
					self.SyncModel(reflect.Zero(lFieldType).Interface())
					//utils.Dbg("222 SyncModel", lFieldType)
				}
				// 添加Extend字段
				lFieldTable := self.Tables[lFieldType]
				if lFieldTable == nil {
					utils.Logger.Error("extends failed!")
				}

				for _, fld := range lFieldTable.Fields {
					//d待续
					if _, has := lTable.Fields[fld.Name]; !has {
						//lNewFld := fld.New()
						//lNewFld._inherit = true
						//lNewFld.cokey_field_name=
						//lNewFld.relkey_field_name=
						lTable.Fields[fld.Name] = fld
						//utils.Dbg("5999fld", fld.Name)

						if fld.primary_key && fld.auto_increment {
							lTable.RecordField = fld
							//utils.Dbg("RecordField", fld.Name)
						}

						// 记录继承字段
						//lModelName := utils.TitleCasedName(utils.DotCasedName(lFieldTable.Name))
						//lTable.InheritFields[fld.Name] = NewRelateField(fld.Name, lModelName, "", fld, "")
					}
				}
				/* 废弃
					//添加原始库到
					lOrgFieldTable := self.Engine.Tables[lFieldType]
					if lOrgFieldTable == nil {
						utils.Logger.Error("extends failed!")
					}
					for _, col := range lOrgFieldTable.Columns() {
						if c := lOrgTable.GetColumn(col.Name); c == nil {
							lOrgTable.AddColumn(col)
							utils.Dbg("5999AddColumn", col.Name)
						}

						// 为新表添加Key字段
						//if col.IsAutoIncrement && col.IsPrimaryKey {
						//	lTable.RecordField = lField
						//	utils.Dbg("5999RecordField", lField.Name)
						//}
					}*/
				fallthrough // 继续其他Case
			case "inherits": // postgres 的继承功能
				if !utils.InStrings(lFieldName, lTable.Inherits...) {
					lTable.Inherits = append(lTable.Inherits, lFieldName)
				}
				break
				// 字段其他参数映射
			case "inherited": // 该字段继承来自X表X字段名称 //name = openerp.fields.Char(related='partner_id.name', inherited=True)
				// inherited(partner_id.name)
				//TODO 实现
				break
			case "_relate": // 关联某表
				if len(lTag) > 1 {
					utils.Dbg("relate to:", utils.DotCasedName(lMemberName), utils.SnakeCasedName(lTag[1]))
					// 现在成员名是关联的Model名,Tag 为关联的字段
					lTable.Relations[utils.DotCasedName(lMemberName)] = utils.SnakeCasedName(lTag[1])
				}
			case "related": //废弃 该字段使Model继承父级依据 See SetupModels()
				//  更新关联字段名称
				lField.related = true
				if len(lTag) > 1 {
					utils.Dbg("Relations:", lTag[1])
					//lField._inherit = utils.StrToBool(lTag[1])
					//lField.related = utils.StrToBool(lTag[1]) // 表面关联的
					lTable.Relations[lTag[1]] = lField.Name
				}

			case "pk":
				lCol.IsPrimaryKey = true
				lField.primary_key = true
			case "autoincr", "auto":
				lCol.IsAutoIncrement = true
				lField.auto_increment = true
			case "default":
				if len(lTag) > 1 {
					utils.Dbg("default:", lTag[1])
					lCol.Default = lTag[1]
				}
			case "created":
				lCol.IsCreated = true
			case "updated":
				lCol.IsUpdated = true
			case "deleted":
				lCol.IsDeleted = true
			case "index":
				//lCol.isIsDeleted = true
				break
			case "unique":
				// 变更XORM
				if index, ok := lOrgTable.Indexes[lCol.Name]; ok {
					index.AddColumn(lCol.Name)
					lCol.Indexes[index.Name] = true
				} else {
					index := core.NewIndex(lCol.Name, core.UniqueType)
					index.AddColumn(lCol.Name)
					lOrgTable.AddIndex(index)
					lCol.Indexes[index.Name] = true
				}
				break
			// 字段类型映射
			case "bool": // 布尔
				lCol.SQLType = core.SQLType{core.Bool, 0, 0}
				lField._type = "boolean"
				lField.Type = "boolean"
			case "int": //数值
				lCol.SQLType = core.SQLType{core.Int, 0, 0}
				lField._type = "integer"
				lField.Type = "integer"
			case "bigint": //数值
				lCol.SQLType = core.SQLType{core.BigInt, 0, 0}
				lField._type = "integer"
				lField.Type = "integer"
			case "float": //浮点
				lCol.SQLType = core.SQLType{core.Float, 0, 0}
				lField._type = "float"
				lField.Type = "float"
			case "double": //浮点
				lCol.SQLType = core.SQLType{core.Double, 0, 0}
				lField._type = "float"
				lField.Type = "float"
			case "text": // 文本
				lCol.SQLType = core.SQLType{core.Text, 0, 0}
				lField._type = "text"
				lField.Type = "text"
				lField._symbol_c = `'%s'`
				lField._symbol_f = _CharFormat
			case "char", "varchar": // 字符串
				lCol.SQLType = core.SQLType{core.Varchar, 0, 0}
				lField._type = "char"
				lField.Type = "char"
				lField._symbol_c = `'%s'`
				lField._symbol_f = _CharFormat
			case "datetime":
				lCol.SQLType = core.SQLType{core.DateTime, 0, 0}
				lField._type = "datetime"
				lField.Type = "datetime"
			case "binary":
				lCol.SQLType = core.SQLType{core.Binary, 0, 0}
				lField._type = "binary"
				lField.Type = "binary"
			case "json":
				lCol.SQLType = core.SQLType{core.Text, 0, 0}
				lField._type = "json"
				lField.Type = "json"
			case "one2many": // 用户有多少权限
				// 不直接指定 采用以下tag写法
				// field:"one2many() int()"
				//lField.initOne2Many(lTag[1:]...)
				self.tag_one2many(lField, lTag[1:]...)
			case "many2one": //用户有多少公司
				// 不直接指定 采用以下tag写法
				// field:"many2one() int()"
				//col.SQLType = core.Type2SQLType(lFieldType)
				//lField.initMany2One(lTag[1:]...)
				self.tag_many2one(lField, lTag[1:]...)
			case "many2many":
				// 不直接指定 采用以下tag写法
				// field:"many2many() int()"
				//lField.initMany2Many(lTag[1:]...)
				self.tag_many2many(lField, lTag[1:]...)
			case "selection":
				//fields.Selection([('linear', 'Linear'), ('degressive', 'Degressive')]), string='Computation Method'
				//fields.Selection(['linear', 'Linear','degressive', 'Degressive']), string='Computation Method'
				lField.Type = "selection"
				//lField.initSelection(lTag[1:]...)
				self.tag_selection(v, lField, lTag[1:]...)
			case "function": // fields.function(_get_full_name
				//lField.Type = "function" function 是未定义字段
				self.tag_function(lField, lTag[1:]...)
			case "name": // title of the field
				if len(lTag) > 1 {
					lNewName := lTag[1]

					//  更新关联字段名称
					for tbl, fld := range lTable.Relations {
						if fld == lField.Name {

							lTable.Relations[tbl] = lNewName
							break
						}
					}

					// 完成修改
					lCol.Name = lNewName
					lField.Name = lNewName
				}
			case "string": // title of the field
				if len(lTag) > 1 {
					lField.String = strings.Trim(lTag[1], "'")
					lField.String = strings.Replace(lField.String, "''", "'", -1)
				}
			case "help":
				if len(lTag) > 1 {
					lField.Help = strings.Trim(lTag[1], "'")
					lField.String = strings.Replace(lField.String, "''", "'", -1)
					//utils.Dbg(lField.Help)
				}
			case "required": // required(true)
				lField.Required = true
				lCol.Nullable = !lField.Required
				if len(lTag) > 1 {
					lField.Required = utils.StrToBool(lTag[1])
				}
			case "readonly": // readonly(true)
				lField.Readonly = true
				//lCol.MapType = core.ONLYFROMDB
				if len(lTag) > 1 {
					lField.Readonly = utils.StrToBool(lTag[1])
				}
			case "states": // states={'draft': [('readonly', False)]}
				break
			case "priority":
				break
			case "size":
				if len(lTag) > 1 {
					lCol.Length = int(utils.StrToInt64(lTag[1]))
					lField.Size = utils.StrToInt64(lTag[1])
				}
			case "ondelete": // ondelete(restrict) for m2m
				if len(lTag) > 1 {
					lField.ondelete = lTag[1]
				}
			case "translate": //translate=True
				break
			case "select": //select=True
				break
			case "write":
				lField.write = true
				break
			case "read":
				break
			case "selectable":
			case "group_operator":
			case "groups": // groups='base.group_user' CSV list of ext IDs of groups
			case "deprecated": // # Optional deprecation warning
			default:
				utils.Dbg("unknown tag ", key)
			}
		}

		if lCol != nil {
			if lCol.IsAutoIncrement && lCol.IsPrimaryKey {
				lTable.RecordField = lField
			}
		}

		// 设置Help
		if lField.String == "" {
			lField.String = lField.Name
		}

		if lField.Help == "" && lField.String != "" {
			lField.Help = lField.String
		}
		// 是否继承关联表所有字段
		if lField.related {
			self.tag_related(lTable, lField)
		}

		// 通过条件过滤不学要的原始字段
		if !lIgonre && lCol.SQLType.Name != "" && lField._type != "many2many" {
			lOrgTable.AddColumn(lCol)
		}

		// 添加字段进Table
		utils.Dbg(lField._type, lField.Name)
		if lField._type != "" && lField.Name != "" {
			lTable.Fields[lFieldName] = lField // !!!替代方式
		}
	}

	// 设置关联到外表的字段
	for _, name := range lRelateFields {
		if fld, has := lTable.Fields[name]; has {
			fld.related = true
		}
	}
	// 创建ORM table
	/*
		// 遍历获得原始字段 例如：Extends其他表的字段
		for _, col := range t.Columns() {
			lField := NewField()
			lField.Name = col.Name

			lField._type = FieldTypes[col.SQLType.Name]
			lField.Type = FieldTypes[col.SQLType.Name]
			//utils.Logger.DebugLn("col.SQLType.Name Fields ", lField.Name, col.SQLType.Name)
			table.Fields[col.Name] = lField

			if col.IsAutoIncrement && col.IsPrimaryKey {
				table.RecordField = lField
			}
		}
	*/

	//utils.Logger.DebugLn("SyncModels Fields ", table.Fields)
	//self.Tables[lType] = lTable
	//self.nameIndex[t.Name] = lTable //添加表名称索引
	utils.Dbg("maptype", lTable, lOrgTable, t.Name)
	//self.Engine.Tables[lType] = lOrgTable // 更新原始ORM Table
	return
}

//# 插入一个新的Table并创建
// 同步更新Model 并返回同步后表 <字段>
func (self *TOrm) SyncModel(model interface{}) (table *TTable, err error) {
	sess := self.NewSession()
	defer func() {
		sess.Close()
		self.ShowSQL(TestShowSql) // 恢复
	}()

	// 同步
	self.ShowSQL(false) // 关闭SQL显示
	//err = self.Sync2(model)

	// 原始库解析Model
	lValue := reflect.Indirect(reflect.ValueOf(model))
	lType := lValue.Type()
	lTable := self.TableInfo(model)
	lTables, err := self.DBMetas() //获取原始ORM所有表
	if utils.LogErr(err) {
		return nil, err
	}

	// find exsit table
	var lOrgTable *core.Table
	for _, tb := range lTables {
		if strings.ToLower(tb.Name) == strings.ToLower(lTable.Name) {
			lOrgTable = tb
			break
		}
	}

	// 重新Mapping新规则
	//utils.Dbg("SyncModel1", len(lTables), lOrgTable, lTable.Name)
	var NewTable *TTable
	NewTable, lTable = self.mapType(model, lTable) // 更新自定义后的Table
	// 保存会列表
	self.Tables[lType] = NewTable
	self.nameIndex[NewTable.Name] = NewTable //添加表名称索引
	self.Engine.Tables[lType] = lTable       // 更新原始ORM Table
	//utils.Dbg("maptype", lTable, lOrgTable, t.Name)

	// 如果不存在 创建新的
	if lOrgTable == nil {
		var (
		//lType  reflect.Type
		//lValue reflect.Value
		//lName  string
		)

		//bean := self.mapType(model, lTable) //self.mapType(lValue, lTable)
		//utils.Dbg("SyncModel2", bean)
		err = self.StoreEngine(sess.Statement.StoreEngine).CreateTable(lTable)
		if err != nil {
			return nil, err
		}

		err = self.createUniques(lTable)
		if err != nil {
			return nil, err
		}

		err = self.createIndexes(lTable)
		if err != nil {
			return nil, err
		}
	} else {

		//lTable := self.mapType(model, lTable) //self.mapType(lValue, lTable)
		//self.Engine.Tables[lTable.Type] = lTable // 更新原始ORM Table

		// 添加新的Column
		for _, col := range lTable.Columns() {
			var OrgCol *core.Column
			for _, col2 := range lOrgTable.Columns() {
				if strings.ToLower(col.Name) == strings.ToLower(col2.Name) {
					OrgCol = col2
					break
				}
			}

			//utils.Dbg("SyncModel1 OrgCol", OrgCol, col.Name)

			if OrgCol != nil {
				expectedType := self.Dialect().SqlType(col)
				curType := self.Dialect().SqlType(OrgCol)
				if expectedType != curType {
					if expectedType == core.Text &&
						strings.HasPrefix(curType, core.Varchar) {
						// currently only support mysql & postgres
						if self.Dialect().DBType() == core.MYSQL ||
							self.Dialect().DBType() == core.POSTGRES {
							self.Logger().Warnf("Table %s column %s change type from %s to %s\n",
								lTable.Name, col.Name, curType, expectedType)
							_, err = self.Exec(self.Dialect().ModifyColumnSql(lTable.Name, col))
						} else {
							self.Logger().Warnf("Table %s column %s db type is %s, struct type is %s\n",
								lTable.Name, col.Name, curType, expectedType)
						}
					} else if strings.HasPrefix(curType, core.Varchar) && strings.HasPrefix(expectedType, core.Varchar) {
						if self.Dialect().DBType() == core.MYSQL {
							if OrgCol.Length < col.Length {
								self.Logger().Infof("Table %s column %s change type from varchar(%d) to varchar(%d)\n",
									lTable.Name, col.Name, OrgCol.Length, col.Length)
								_, err = self.Exec(self.Dialect().ModifyColumnSql(lTable.Name, col))
							}
						}
					} else {
						self.Logger().Warnf("Table %s column %s db type is %s, struct type is %s",
							lTable.Name, col.Name, curType, expectedType)
					}
				} else if expectedType == core.Varchar {
					if self.Dialect().DBType() == core.MYSQL {
						if OrgCol.Length < col.Length {
							self.Logger().Infof("Table %s column %s change type from varchar(%d) to varchar(%d)\n",
								lTable.Name, col.Name, OrgCol.Length, col.Length)
							_, err = self.Exec(self.Dialect().ModifyColumnSql(lTable.Name, col))
						}
					}
				}
				if col.Default != OrgCol.Default {
					self.Logger().Warnf("Table %s Column %s db default is %s, struct default is %s",
						lTable.Name, col.Name, OrgCol.Default, col.Default)
				}
				if col.Nullable != OrgCol.Nullable {
					self.Logger().Warnf("Table %s Column %s db nullable is %v, struct nullable is %v",
						lTable.Name, col.Name, OrgCol.Nullable, col.Nullable)
				}
			} else {
				session := self.NewSession()
				session.Statement.RefTable = lTable
				defer session.Close()
				err = session.addColumn(col.Name)
			}
			if err != nil {
				return nil, err
			}
		}

		var foundIndexNames = make(map[string]bool)
		var addedNames = make(map[string]*core.Index)
		// 变更索引
		for name, index := range lTable.Indexes {
			var oriIndex *core.Index
			for name2, index2 := range lOrgTable.Indexes {
				if index.Equal(index2) {
					oriIndex = index2
					foundIndexNames[name2] = true
					utils.Dbg("rrrrrr", name, index, oriIndex.Type, index.Type)
					break
				}
			}

			if oriIndex != nil {
				if oriIndex.Type != index.Type {
					sql := self.Dialect().DropIndexSql(lTable.Name, oriIndex)
					_, err = self.Exec(sql)
					if err != nil {
						//return nil, err
						utils.Logger.Error("func SyncModel error#%s:%s", sql, err.Error())
					}
					oriIndex = nil
				}
			}

			if oriIndex == nil {
				addedNames[name] = index
			}
		}

		for name2, index2 := range lOrgTable.Indexes {
			if _, ok := foundIndexNames[name2]; !ok {
				sql := self.Dialect().DropIndexSql(lTable.Name, index2)
				_, err = self.Exec(sql)
				if err != nil {
					utils.Logger.Error("func SyncModel error#%s:%s", sql, err.Error())
					//return nil, err
				}
			}
		}

		// 唯一键
		for name, index := range addedNames {
			if index.Type == core.UniqueType {
				session := self.NewSession()
				session.Statement.RefTable = lTable
				defer session.Close()
				err = session.addUnique(lTable.Name, name)
			} else if index.Type == core.IndexType {
				session := self.NewSession()
				session.Statement.RefTable = lTable
				defer session.Close()
				err = session.addIndex(lTable.Name, name)
			}
			if err != nil {
				return nil, err
			}
		}

	}
	/*  // 验证Table完整性
	for _, table := range tables {
		var oriTable *core.Table
		for _, structTable := range structTables {
			if equalNoCase(table.Name, structTable.Name) {
				oriTable = structTable
				break
			}
		}

		if oriTable == nil {
			//engine.LogWarnf("Table %s has no struct to mapping it", table.Name)
			continue
		}

		for _, colName := range table.ColumnsSeq() {
			if oriTable.GetColumn(colName) == nil {
				engine.LogWarnf("Table %s has column %s but struct has not related field",
					table.Name, colName)
			}
		}
	}
	*/
	/*
		// 创建ORM table
		var (
			lType  reflect.Type
			lValue reflect.Value
			lName  string
		)
		lValue = reflect.Indirect(reflect.ValueOf(model))
		lType = lValue.Type()
		lName = utils.SnakeCasedName(utils.Obj2Name(model))

		//utils.Logger.DebugLn("col.SQLType.Name Fields", lType, self.Tables[lType])
		// 遍历获得字段
		table = NewTable(lName, lType)
		if tb, has := self.Engine.Tables[lType]; has {

			for _, col := range tb.Columns() {
				lField := NewField()
				lField.Name = col.Name

				lField._type = FieldTypes[col.SQLType.Name]
				lField.Type = FieldTypes[col.SQLType.Name]
				//utils.Logger.DebugLn("col.SQLType.Name Fields ", lField.Name, col.SQLType.Name)
				table.Fields[col.Name] = lField

				if col.IsAutoIncrement && col.IsPrimaryKey {
					table.RecordField = lField
				}
			}
		}

		//utils.Logger.DebugLn("SyncModels Fields ", table.Fields)
		self.Tables[lType] = table
		self.nameIndex[lName] = table

		// 解析自定义字段
		//self.mapType(lValue)
	*/
	table = self.Tables[lTable.Type]
	utils.Dbg("sycnmodel:", table, lTable.Type)
	return table, nil
}

// 执行SQL 并返回主键ID
func (self *TOrm) __SqlInsert(sql string, params ...interface{}) (int64, error) {
	lRes, err := self.Engine.Exec(sql, params...)
	utils.LogErr(err)

	return lRes.LastInsertId()
}

func (self *TOrm) SqlQuery(sql string, params ...string) (ds *TDataSet, err error) {
	// 无论如何都会返回一个Dataset
	ds = NewDataSet()
	ds.KeyField = "id" //设置主键 TODO:可以做到动态

	// 转换为[]interface{}
	t := make([]interface{}, 0)
	for _, str := range params {
		t = append(t, str)
	}

	// 填充
	sql = fmt.Sprintf(sql, t...)

	if TestShowSql {
		utils.Logger.InfoLn("SqlExec:", sql, params)
	}

	//lRows, err := self.Engine.DB().Query(sql, t...)
	lRows, err := self.Engine.DB().Query(sql)
	if utils.LogErr(err) {
		return // nil, err
	}

	defer lRows.Close()
	for lRows.Next() {
		tempMap := make(map[string]interface{})
		err = lRows.ScanMap(&tempMap)
		if !utils.LogErr(err) {
			//res = append(res, tempMap)
			ds.NewRecord(tempMap)
		}

		//utils.Logger.DebugLn("SqlQuery tempMap :", tempMap)
	}

	// 打印错误
	err = lRows.Err()
	utils.LogErr(err)

	return ds, err
}

// 执行SQL 并返回有效行
func (self *TOrm) SqlExec(sql string, params ...interface{}) (sql.Result, error) {
	// 组成SQL
	//sql = fmt.Sprintf(sql, params...)
	if TestShowSql {
		utils.Logger.InfoLn("SqlExec:", sql, params)
	}

	//	lRes, err := self.Engine.Exec(sql, params...)
	//	if utils.LogErr(err) {
	//		return 0, err
	//	}

	//	utils.Logger.DebugLn("exexex", self.DriverName(), strings.Count(strings.ToLower(sql), "returning") == 1, sql)

	// 过滤Pg 的插入语句
	if self.DriverName() == "postgres" && strings.Count(strings.ToLower(sql), "returning") == 1 {
		res, err := self.Engine.Query(sql, params...)
		//utils.Logger.DebugLn("exexex", res, err)
		if !utils.LogErr(err) && len(res) > 0 {
			for key, val := range res[0] {
				id, err := strconv.ParseInt(string(val), 10, 64)
				if !utils.LogErr(err, key) {
					return TExecResult(id), err
				}
			}

		}
		return nil, err
	}

	return self.Engine.Exec(sql, params...)

}

func (self *TOrm) ModelInfo(aType reflect.Type) {
	/*	table, err := self.DBMetas()

		if table, has := self.Tables[aType]; has {
			for _, col := range table.Columns() {

			}
		}
	*/
}

// Temporarily change the Get, Find, Update's table
func (self *TOrm) Table(tableNameOrBean interface{}) *TOrmSession {
	session := self.NewSession()
	session.IsAutoClose = true
	return session.Table(tableNameOrBean)
}

// set store engine when create table, only support mysql now
func (self *TOrm) StoreEngine(storeEngine string) *TOrmSession {
	session := self.NewSession()
	session.IsAutoClose = true
	return session.StoreEngine(storeEngine)
}

// Method core.Table can input a string or pointer to struct for special a table to operate.
func (self *TOrmSession) Table(tableNameOrBean interface{}) *TOrmSession {
	self.Statement.Table(tableNameOrBean)
	return self
}

// Prepare
func (self *TOrmSession) Prepare() *TOrmSession {
	self.Session.Prepare()
	return self
}

// Method Sql provides raw sql input parameter. When you have a complex SQL statement
// and cannot use Where, Id, In and etc. Methods to describe, you can use Sql.
func (self *TOrmSession) Sql(querystring string, args ...interface{}) *TOrmSession {
	self.Statement.Sql(querystring, args...)
	return self
}

// Method Where provides custom query condition.
func (self *TOrmSession) Where(querystring string, args ...interface{}) *TOrmSession {
	self.Statement.Where(querystring, args...)
	return self
}

// Method Where provides custom query condition.
func (self *TOrmSession) And(querystring string, args ...interface{}) *TOrmSession {
	self.Statement.And(querystring, args...)
	return self
}

// Method Where provides custom query condition.
func (self *TOrmSession) Or(querystring string, args ...interface{}) *TOrmSession {
	self.Statement.Or(querystring, args...)
	return self
}

// Method Id provides converting id as a query condition
func (self *TOrmSession) Id(id interface{}) *TOrmSession {
	self.Statement.Id(id)
	return self
}

// Apply before Processor, affected bean is passed to closure arg
func (self *TOrmSession) Before(closures func(interface{})) *TOrmSession {
	self.Session.Before(closures)
	return self
}

// Apply after Processor, affected bean is passed to closure arg
func (self *TOrmSession) After(closures func(interface{})) *TOrmSession {
	self.Session.After(closures)
	return self
}

// set the table alias
func (self *TOrmSession) Alias(alias string) *TOrmSession {
	self.Statement.Alias(alias)
	return self
}

// Method In provides a query string like "id in (1, 2, 3)"
func (self *TOrmSession) In(column string, args ...interface{}) *TOrmSession {
	self.Statement.In(column, args...)
	return self
}

// Method In provides a query string like "count = count + 1"
func (self *TOrmSession) Incr(column string, arg ...interface{}) *TOrmSession {
	self.Statement.Incr(column, arg...)
	return self
}

// Method Decr provides a query string like "count = count - 1"
func (self *TOrmSession) Decr(column string, arg ...interface{}) *TOrmSession {
	self.Statement.Decr(column, arg...)
	return self
}

// Method SetExpr provides a query string like "column = {expression}"
func (self *TOrmSession) SetExpr(column string, expression string) *TOrmSession {
	self.Statement.SetExpr(column, expression)
	return self
}

// Method Cols provides some columns to special
func (self *TOrmSession) Select(str string) *TOrmSession {
	self.Statement.Select(str)
	return self
}

// Method Cols provides some columns to special
func (self *TOrmSession) Cols(columns ...string) *TOrmSession {
	self.Statement.Cols(columns...)
	return self
}

func (self *TOrmSession) AllCols() *TOrmSession {
	self.Statement.AllCols()
	return self
}

func (self *TOrmSession) MustCols(columns ...string) *TOrmSession {
	self.Statement.MustCols(columns...)
	return self
}

func (self *TOrmSession) NoCascade() *TOrmSession {
	self.Statement.UseCascade = false
	return self
}

// Xorm automatically retrieve condition according struct, but
// if struct has bool field, it will ignore them. So use UseBool
// to tell system to do not ignore them.
// If no paramters, it will use all the bool field of struct, or
// it will use paramters's columns
func (self *TOrmSession) UseBool(columns ...string) *TOrmSession {
	self.Statement.UseBool(columns...)
	return self
}

// use for distinct columns. Caution: when you are using cache,
// distinct will not be cached because cache system need id,
// but distinct will not provide id
func (self *TOrmSession) Distinct(columns ...string) *TOrmSession {
	self.Statement.Distinct(columns...)
	return self
}

// Set Read/Write locking for UPDATE
func (self *TOrmSession) ForUpdate() *TOrmSession {
	self.Statement.IsForUpdate = true
	return self
}

// Only not use the paramters as select or update columns
func (self *TOrmSession) Omit(columns ...string) *TOrmSession {
	self.Statement.Omit(columns...)
	return self
}

// Set null when column is zero-value and nullable for update
func (self *TOrmSession) Nullable(columns ...string) *TOrmSession {
	self.Statement.Nullable(columns...)
	return self
}

// Method NoAutoTime means do not automatically give created field and updated field
// the current time on the current session temporarily
func (self *TOrmSession) NoAutoTime() *TOrmSession {
	self.Statement.UseAutoTime = false
	return self
}

func (self *TOrmSession) NoAutoCondition(no ...bool) *TOrmSession {
	self.Statement.NoAutoCondition(no...)
	return self
}

// Method Limit provide limit and offset query condition
func (self *TOrmSession) Limit(limit int, start ...int) *TOrmSession {
	self.Statement.Limit(limit, start...)
	return self
}

// Method OrderBy provide order by query condition, the input parameter is the content
// after order by on a sql statement.
func (self *TOrmSession) OrderBy(order string) *TOrmSession {
	self.Statement.OrderBy(order)
	return self
}

// Method Desc provide desc order by query condition, the input parameters are columns.
func (self *TOrmSession) Desc(colNames ...string) *TOrmSession {
	self.Statement.Desc(colNames...)
	return self
}

// Method Asc provide asc order by query condition, the input parameters are columns.
func (self *TOrmSession) Asc(colNames ...string) *TOrmSession {
	self.Statement.Asc(colNames...)
	return self
}

// Method StoreEngine is only avialble mysql dialect currently
func (self *TOrmSession) StoreEngine(storeEngine string) *TOrmSession {
	self.Statement.StoreEngine = storeEngine
	return self
}

// Method Charset is only avialble mysql dialect currently
func (self *TOrmSession) Charset(charset string) *TOrmSession {
	self.Statement.Charset = charset
	return self
}

// Method Cascade indicates if loading sub Struct
func (self *TOrmSession) Cascade(trueOrFalse ...bool) *TOrmSession {
	if len(trueOrFalse) >= 1 {
		self.Statement.UseCascade = trueOrFalse[0]
	}
	return self
}

// Method NoCache ask this session do not retrieve data from cache system and
// get data from database directly.
func (self *TOrmSession) NoCache() *TOrmSession {
	self.Statement.UseCache = false
	return self
}

//The join_operator should be one of INNER, LEFT OUTER, CROSS etc - this will be prepended to JOIN
func (self *TOrmSession) Join(join_operator string, tablename interface{}, condition string) *TOrmSession {
	self.Statement.Join(join_operator, tablename, condition)
	return self
}

// Generate Group By statement
func (self *TOrmSession) GroupBy(keys string) *TOrmSession {
	self.Statement.GroupBy(keys)
	return self
}

// Generate Having statement
func (self *TOrmSession) Having(conditions string) *TOrmSession {
	self.Statement.Having(conditions)
	return self
}

func (self *TOrmSession) Query(sql string, params ...string) (ds *TDataSet, err error) {
	// 转换为[]interface{}
	t := make([]interface{}, 0)
	for _, str := range params {
		t = append(t, str)
	}

	// 填充
	sql = fmt.Sprintf(sql, t...)
	utils.Logger.DebugLn("SqlQuery:", sql, params)

	//lRows, err := self.Engine.DB().Query(sql, t...)
	lRows, err := self.Engine.DB().Query(sql)

	if utils.LogErr(err) {
		return nil, err
	}

	ds = NewDataSet()
	ds.KeyField = "id" //设置主键

	defer lRows.Close()
	for lRows.Next() {
		tempMap := make(map[string]interface{})
		err = lRows.ScanMap(&tempMap)
		if !utils.LogErr(err) {
			//res = append(res, tempMap)
			ds.NewRecord(tempMap)
		}

		//utils.Logger.DebugLn("SqlQuery tempMap :", tempMap)
	}

	// 打印错误
	err = lRows.Err()
	utils.LogErr(err)

	return ds, err
}

// 执行SQL 并返回有效行
func (self *TOrmSession) Exec(sql string, params ...interface{}) (sql.Result, error) {
	// 组成SQL
	//sql = fmt.Sprintf(sql, params...)
	//utils.Logger.DebugLn("SqlExec:", sql, params)

	//	lRes, err := self.Engine.Exec(sql, params...)
	//	if utils.LogErr(err) {
	//		return 0, err
	//	}
	//lTableNaame := self.Statement.TableName()
	//lTable := self.Orm.TableByName(lTableNaame)

	//self._Validate(lTable)
	//fmt.Println("_Validate", lTableNaame, lTable)

	// 过滤Pg 的插入语句
	utils.Logger.DebugLn("exexex", self.Orm.DriverName(), strings.Count(strings.ToLower(sql), "returning") == 1, sql)
	if self.Orm.DriverName() == "postgres" && strings.Count(strings.ToLower(sql), "returning") == 1 {

		res, err := self.Engine.Query(sql, params...)
		//utils.Logger.DebugLn("exexex", res, err)
		if !utils.LogErr(err) && len(res) > 0 {
			for key, val := range res[0] {
				id, err := strconv.ParseInt(string(val), 10, 64)
				if !utils.LogErr(err, key) {
					return TExecResult(id), err
				}
			}

		}
		return nil, err
	}

	return self.Engine.Exec(sql, params...)

}

func (self *TOrmSession) resetStatement() {
	if self.AutoResetStatement {
		self.Statement.Init()
	}
}

func (self *TOrmSession) addColumn(colName string) error {
	defer self.resetStatement()
	if self.IsAutoClose {
		defer self.Close()
	}

	col := self.Statement.RefTable.GetColumn(colName)
	sql, args := self.genAddColumnStr(col)
	_, err := self.Exec(sql, args...)
	return err
}

func (self *TOrmSession) addIndex(tableName, idxName string) error {
	defer self.resetStatement()
	if self.IsAutoClose {
		defer self.Close()
	}

	index := self.Statement.RefTable.Indexes[idxName]
	sqlStr := self.Engine.Dialect().CreateIndexSql(tableName, index)

	_, err := self.Exec(sqlStr)
	return err
}

func (self *TOrmSession) addUnique(tableName, uqeName string) error {
	defer self.resetStatement()
	if self.IsAutoClose {
		defer self.Close()
	}
	index := self.Statement.RefTable.Indexes[uqeName]
	sqlStr := self.Engine.Dialect().CreateIndexSql(tableName, index)
	_, err := self.Exec(sqlStr)
	return err
}

// this function create a table according a bean
func (self *TOrmSession) CreateTable(bean *core.Table) error {
	self.Statement.RefTable = bean //session.Engine.TableInfo(bean)

	defer self.resetStatement()
	if self.IsAutoClose {
		defer self.Close()
	}

	return self.createOneTable()
}

// create indexes
func (self *TOrm) createIndexes(bean *core.Table) error {
	sess := self.NewSession()
	if sess.IsAutoClose {
		defer sess.Close()
	}
	defer sess.resetStatement()

	sess.Statement.RefTable = bean // session.Engine.TableInfo(bean)
	if sess.IsAutoClose {
		defer sess.Close()
	}

	sqls := sess.genIndexSQL()
	for _, sqlStr := range sqls {
		_, err := sess.Exec(sqlStr)
		if err != nil {
			return err
		}
	}
	return nil
}

// create uniques
func (self *TOrm) createUniques(bean *core.Table) error {
	sess := self.NewSession()
	if sess.IsAutoClose {
		defer sess.Close()
	}
	defer sess.resetStatement()

	sess.Statement.RefTable = bean // session.Engine.TableInfo(bean)
	sqls := sess.genUniqueSQL()
	for _, sqlStr := range sqls {
		_, err := sess.Exec(sqlStr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *TOrmSession) genAddColumnStr(col *core.Column) (string, []interface{}) {
	quote := self.Engine.Quote
	sql := fmt.Sprintf("ALTER TABLE %v ADD %v;", quote(self.Statement.TableName()),
		col.String(self.Engine.Dialect()))
	return sql, []interface{}{}
}

func (self *TOrmSession) genIndexSQL() []string {
	var sqls []string = make([]string, 0)
	tbName := self.Statement.TableName()
	quote := self.Engine.Quote
	for idxName, index := range self.Statement.RefTable.Indexes {
		if index.Type == core.IndexType {
			sql := fmt.Sprintf("CREATE INDEX %v ON %v (%v);", quote(fmt.Sprintf("IDX_%v_%v", self.Statement.RefTable.Name, idxName)),
				quote(tbName), quote(strings.Join(index.Cols, quote(","))))
			sqls = append(sqls, sql)
		}
	}
	return sqls
}

func (self *TOrmSession) genUniqueSQL() []string {
	var sqls []string = make([]string, 0)
	for _, index := range self.Statement.RefTable.Indexes {
		if index.Type == core.UniqueType {
			sql := self.Engine.Dialect().CreateIndexSql(self.Statement.RefTable.Name, index)
			sqls = append(sqls, sql)
		}
	}
	return sqls
}

func (self *TOrmSession) createOneTable() error {
	//sqlStr := self.Statement.genCreateTableSQL()
	sqlStr := self.Engine.Dialect().CreateTableSql(self.Statement.RefTable, self.Statement.AltTableName,
		self.Statement.StoreEngine, self.Statement.Charset)

	// 实现PG的继承
	lTable := self.Orm.Tables[self.Statement.RefTable.Type]
	if lTable != nil {
		//lInherits := lTable.Inherits
		if len(lTable.Inherits) > 0 && strings.EqualFold(self.Engine.DriverName(), "postgres") {
			sqlStr += "INHERITS  ( "
			sqlStr += strings.Join(lTable.Inherits, ",")
			sqlStr += " ) "
		}
	}
	utils.Dbg("createOneTable", lTable, sqlStr)
	_, err := self.Exec(sqlStr)
	return err
}
