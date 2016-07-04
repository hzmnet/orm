package orm

/** 数据集
 */

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
	"webgo/logger"
	"webgo/utils"
)

type (
	TFieldSet struct {
		DataSet *TDataSet
		RecSet  *TRecordSet
		Name    string
	}

	TRecordSet struct {
		DataSet   *TDataSet
		Fields    []string
		Values    []string
		NameIndex map[string]int
		Length    int
	}

	TDataSet struct {
		Data   []*TRecordSet         // []map[string]interface{}
		Fields map[string]*TFieldSet //保存字段
		//Delta // 修改过的
		KeyField     string                 // 主键字段
		RecordsIndex map[string]*TRecordSet // 主键引索
		Position     int                    // 游标
		//Count int

		FieldCount int //字段数
	}
)

func NewRecordSet(dataSet *TDataSet) *TRecordSet {
	return &TRecordSet{
		DataSet:   dataSet,
		Fields:    make([]string, 0),
		Values:    make([]string, 0),
		NameIndex: make(map[string]int),
		Length:    0,
	}
}

func NewDataSet() *TDataSet {
	return &TDataSet{
		Position:     0,
		KeyField:     "id",
		Data:         make([]*TRecordSet, 0),
		Fields:       make(map[string]*TFieldSet),
		RecordsIndex: make(map[string]*TRecordSet),
		//Count: 0,
	}
}

func (self *TFieldSet) AsString(src ...string) string {
	//RecSet := self.DataSet.Data[self.DataSet.Position]

	if len(src) != 0 {
		self.RecSet._setByName(self.Name, src[0])
		return src[0]
	}
	//fmt.Println("AsString", self, self.Name)
	return self.RecSet._getByName(self.Name)
}

func (self *TFieldSet) AsInteger(src ...int64) int64 {
	//RecSet := self.DataSet.Data[self.DataSet.Position]

	if len(src) != 0 {
		self.RecSet._setByName(self.Name, utils.IntToStr(src[0]))
		return src[0]
	}
	return utils.StrToInt64(self.RecSet._getByName(self.Name))
}

func (self *TFieldSet) AsBoolean(src ...bool) bool {
	//RecSet := self.DataSet.Data[self.DataSet.Position]

	if len(src) != 0 {
		self.RecSet._setByName(self.Name, utils.BoolToStr(src[0]))
		return src[0]
	}
	return utils.StrToBool(self.RecSet._getByName(self.Name))
}

func (self *TFieldSet) AsDateTime(src ...time.Time) (t time.Time) {
	//RecSet := self.DataSet.Data[self.DataSet.Position]
	if len(src) != 0 {
		self.RecSet._setByName(self.Name, src[0].Format(time.RFC3339))
		return src[0]
	}
	t, _ = time.Parse(time.RFC3339, self.RecSet._getByName(self.Name))
	return
}

func (self *TFieldSet) AsFloat(src ...float64) float64 {
	//RecSet := self.DataSet.Data[self.DataSet.Position]

	if len(src) != 0 {
		self.RecSet._setByName(self.Name, utils.FloatToStr(src[0]))
		return src[0]
	}

	return utils.StrToFloat(self.RecSet._getByName(self.Name))
}

func (self *TRecordSet) Get(index int) string {
	if index >= self.Length {
		return ""
	}
	//fmt.Println("_getByName Get", index, self.Values)
	return self.Values[index]
}

func (self *TRecordSet) Set(index int, value string) bool {
	if index >= self.Length {
		return false
	}
	self.Values[index] = value
	return true
}

func (self *TRecordSet) _getByName(name string) string {
	//fmt.Println("_getByName", self.NameIndex)
	if index, ok := self.NameIndex[name]; ok {
		//fmt.Println("_getByName", index, self.Get(index))
		return self.Get(index)
	}
	return ""
}

func (self *TRecordSet) _setByName(name string, value string) bool {
	if index, ok := self.NameIndex[name]; ok {
		return self.Set(index, value)
	} else {
		self.NameIndex[name] = len(self.Values)
		self.Fields = append(self.Fields, name)
		self.Values = append(self.Values, value)
		self.Length = len(self.Values)
	}
	return true
}

func (self *TRecordSet) GetByIndex(index int) (res *TFieldSet) {
	// 检查零界
	if index >= self.Length && len(self.DataSet.Fields) != self.Length {
		return
	}

	field := self.Fields[index]
	if field != "" {
		res = self.DataSet.Fields[field]
		res.RecSet = self
		return //self.Values[index]
	}

	return nil
}

// 获取某个
func (self *TRecordSet) GetByName(name string) (field *TFieldSet) {
	var has bool
	if field, has = self.DataSet.Fields[name]; has {
		if field != nil {
			field.RecSet = self
			return //self.Values[index]
		}
	}
	return
}

func (self *TRecordSet) ToStrMap() (res map[string]string) {
	res = make(map[string]string)

	for idx, field := range self.Fields {
		res[field] = self.Values[idx]
	}

	return
}

func (self *TRecordSet) ToItfMap() (res map[string]interface{}) {
	res = make(map[string]interface{})

	for idx, field := range self.Fields {
		res[field] = self.Values[idx]
	}

	return
}

func (self *TRecordSet) MergeToStrMap(target map[string]string) (res map[string]string) {
	for idx, field := range self.Fields {
		target[field] = self.Values[idx]
	}

	return target
}

func (self *TRecordSet) AsStrMap() (res map[string]string) {
	res = make(map[string]string)
	for idx, field := range self.Fields {
		res[field] = self.Values[idx]
	}

	return
}

//TODO  当TDataSet无数据是返回错误
func (self *TDataSet) FieldByName(field string) (fieldSet *TFieldSet) {
	/*fieldSet = &TFieldSet{
		DataSet: self,
		RecSet:  self.Data[self.Position],
		Name:    field,
	}
	*/
	var has bool
	if fieldSet, has = self.Fields[field]; has {
		//fmt.Println("FieldByName has", fieldSet, fieldSet)
		fieldSet.RecSet = self.Data[self.Position]
		return
	}
	return
}

//
func (self *TDataSet) IsEmpty() bool {
	return len(self.Data) == 0
}

//
func (self *TDataSet) Count() int {
	return len(self.Data)
}

func (self *TDataSet) First() {
	self.Position = 0
}

func (self *TDataSet) Next() {
	self.Position++
}

func (self *TDataSet) Eof() bool {
	return self.Position == len(self.Data)
}

func (self *TDataSet) Record() *TRecordSet {
	return self.Data[self.Position]
}

//push row to dataset
func (self *TDataSet) NewRecord(Record map[string]interface{}) bool {
	//var lRec *TRecordSet
	lRec := NewRecordSet(self)
	var err error
	lValue := ""
	for field, val := range Record {
		if val == nil {
			lValue = ""
		} else {
			rawValue := reflect.Indirect(reflect.ValueOf(val))
			//if row is null then ignore
			if rawValue.Interface() == nil {
				continue
			}

			lValue, err = val2Str(&rawValue)
			if logger.LogErr(err) {
				return false
			}

		}

		//Record[field] = data
		lRec.NameIndex[field] = len(lRec.Fields) // 先于 lRec.Fields 添加不需 -1
		lRec.Fields = append(lRec.Fields, field)
		lRec.Values = append(lRec.Values, lValue)

		if self.KeyField != "" {
			if field == self.KeyField || field == "id" {
				self.RecordsIndex[lValue] = lRec //保存ID 对应的 Record
			}
		}

	}

	// 添加字段长度
	if self.FieldCount == 0 {
		self.FieldCount = len(lRec.Fields)
	}

	//TODO 迁移到其他地方初始化
	// 记录该数据集的字段
	if self.Count() < 1 {
		for _, field := range lRec.Fields {
			fieldSet := &TFieldSet{
				DataSet: self,
				//RecSet:  self.Data[self.Position],
				Name: field,
			}
			self.Fields[field] = fieldSet
		}
	}

	//self.Data = append(self.Data, Record)
	lRec.Length = len(lRec.Values) // 更新记录列数
	self.Data = append(self.Data, lRec)
	return true
}

func (self *TDataSet) DeleteRecord(Key string) bool {
	return true
}

//考虑
func (self *TDataSet) EditRecord(Key string, Record map[string]interface{}) bool {
	return true
}

func (self *TDataSet) RecordByField(field string, val interface{}) (rec *TRecordSet) {
	if field == "" || val == nil {
		return nil
	}

	for _, rec = range self.Data {
		i := rec.NameIndex[field]
		if equal2Str(rec.Values[i], val) {
			return rec
		}
	}
	return
}

// 获取对应KeyFieldd值
func (self *TDataSet) RecordByKey(Key string) (rec *TRecordSet) {
	if self.KeyField == "" {
		return nil
	}

	//idx := self.RecordsIndex[Key]
	return self.RecordsIndex[Key]
}

// 返回所有记录的主键值
func (self *TDataSet) Keys() (res []string) {
	res = make([]string, 0)
	for key, _ := range self.RecordsIndex {
		res = append(res, key)
	}

	return
}

func equal2Str(val1 string, val2 interface{}) bool {
	rawValue := reflect.Indirect(reflect.ValueOf(val2))
	//if row is null then ignore
	if rawValue.Interface() == nil {

	}
	lValue, err := val2Str(&rawValue)
	if err != nil {
		return false
	}

	return val1 == lValue
}

func val2Str(rawValue *reflect.Value) (data string, err error) {
	data, err = rft2val(rawValue)
	if err != nil {
		return
	}
	return
}

func rft2val(rawValue *reflect.Value) (str string, err error) {
	aa := reflect.TypeOf((*rawValue).Interface())
	vv := reflect.ValueOf((*rawValue).Interface())
	switch aa.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(vv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(vv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		str = strconv.FormatFloat(vv.Float(), 'f', -1, 64)
	case reflect.String:
		str = vv.String()
	case reflect.Array, reflect.Slice:
		switch aa.Elem().Kind() {
		case reflect.Uint8:
			data := rawValue.Interface().([]byte)
			str = string(data)
		default:
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	//时间类型
	case reflect.Struct:
		var c_TIME_DEFAULT time.Time
		TimeType := reflect.TypeOf(c_TIME_DEFAULT)
		if aa.ConvertibleTo(TimeType) {
			str = vv.Convert(TimeType).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	case reflect.Bool:
		str = strconv.FormatBool(vv.Bool())
	case reflect.Complex128, reflect.Complex64:
		str = fmt.Sprintf("%v", vv.Complex())
	/* TODO: unsupported types below
	   case reflect.Map:
	   case reflect.Ptr:
	   case reflect.Uintptr:
	   case reflect.UnsafePointer:
	   case reflect.Chan, reflect.Func, reflect.Interface:
	*/
	default:
		err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
	}
	return
}
