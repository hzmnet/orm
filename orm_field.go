package orm

//	"reflect"
import (
	"webgo/logger"
	"webgo/utils"
)

var (
	FieldTypes = map[string]string{
		// 布尔
		"BOOL": "boolean",
		// 整数
		"INT":     "integer",
		"INTEGER": "integer",
		"BIGINT":  "integer",

		"CHAR":     "char",
		"VARCHAR":  "char",
		"NVARCHAR": "char",
		"TEXT":     "text",

		"MEDIUMTEXT": "text",
		"LONGTEXT":   "text",

		"DATE":       "date",
		"DATETIME":   "datetime",
		"TIME":       "datetime",
		"TIMESTAMP":  "datetime",
		"TIMESTAMPZ": "datetime",

		//Decimal = "DECIMAL"
		//Numeric = "NUMERIC"
		"REAL":   "float",
		"FLOAT":  "float",
		"DOUBLE": "float",

		"VARBINARY":  "binary",
		"TINYBLOB":   "binary",
		"BLOB":       "binary",
		"MEDIUMBLOB": "binary",
		"LONGBLOB":   "binary",
		"JSON":       "json",
		"reference":  "reference",
	}
)

type (
	/* The field descriptor contains the field definition, and manages accesses
	   and assignments of the corresponding field on records. The following
	   attributes may be provided when instanciating a field:

	   :param string: the label of the field seen by users (string); if not
	       set, the ORM takes the field name in the class (capitalized).

	   :param help: the tooltip of the field seen by users (string)

	   :param readonly: whether the field is readonly (boolean, by default ``False``)

	   :param required: whether the value of the field is required (boolean, by
	       default ``False``)

	   :param index: whether the field is indexed in database (boolean, by
	       default ``False``)

	   :param default: the default value for the field; this is either a static
	       value, or a function taking a recordset and returning a value

	   :param states: a dictionary mapping state values to lists of UI attribute-value
	       pairs; possible attributes are: 'readonly', 'required', 'invisible'.
	       Note: Any state-based condition requires the ``state`` field value to be
	       available on the client-side UI. This is typically done by including it in
	       the relevant views, possibly made invisible if not relevant for the
	       end-user.

	   :param groups: comma-separated list of group xml ids (string); this
	       restricts the field access to the users of the given groups only

	   :param bool copy: whether the field value should be copied when the record
	       is duplicated (default: ``True`` for normal fields, ``False`` for
	       ``one2many`` and computed fields, including property fields and
	       related fields)

	   :param string oldname: the previous name of this field, so that ORM can rename
	       it automatically at migration

	   .. _field-computed:

	   .. rubric:: Computed fields

	   One can define a field whose value is computed instead of simply being
	   read from the database. The attributes that are specific to computed
	   fields are given below. To define such a field, simply provide a value
	   for the attribute ``compute``.

	   :param compute: name of a method that computes the field

	   :param inverse: name of a method that inverses the field (optional)

	   :param search: name of a method that implement search on the field (optional)

	   :param store: whether the field is stored in database (boolean, by
	       default ``False`` on computed fields)

	   :param compute_sudo: whether the field should be recomputed as superuser
	       to bypass access rights (boolean, by default ``False``)

	   The methods given for ``compute``, ``inverse`` and ``search`` are model
	   methods. Their signature is shown in the following example::

	       upper = fields.Char(compute='_compute_upper',
	                           inverse='_inverse_upper',
	                           search='_search_upper')

	       @api.depends('name')
	       def _compute_upper(self):
	           for rec in self:
	               rec.upper = rec.name.upper() if rec.name else False

	       def _inverse_upper(self):
	           for rec in self:
	               rec.name = rec.upper.lower() if rec.upper else False

	       def _search_upper(self, operator, value):
	           if operator == 'like':
	               operator = 'ilike'
	           return [('name', operator, value)]

	   The compute method has to assign the field on all records of the invoked
	   recordset. The decorator :meth:`openerp.api.depends` must be applied on
	   the compute method to specify the field dependencies; those dependencies
	   are used to determine when to recompute the field; recomputation is
	   automatic and guarantees cache/database consistency. Note that the same
	   method can be used for several fields, you simply have to assign all the
	   given fields in the method; the method will be invoked once for all
	   those fields.

	   By default, a computed field is not stored to the database, and is
	   computed on-the-fly. Adding the attribute ``store=True`` will store the
	   field's values in the database. The advantage of a stored field is that
	   searching on that field is done by the database itself. The disadvantage
	   is that it requires database updates when the field must be recomputed.

	   The inverse method, as its name says, does the inverse of the compute
	   method: the invoked records have a value for the field, and you must
	   apply the necessary changes on the field dependencies such that the
	   computation gives the expected value. Note that a computed field without
	   an inverse method is readonly by default.

	   The search method is invoked when processing domains before doing an
	   actual search on the model. It must return a domain equivalent to the
	   condition: ``field operator value``.

	   .. _field-related:

	   .. rubric:: Related fields

	   The value of a related field is given by following a sequence of
	   relational fields and reading a field on the reached model. The complete
	   sequence of fields to traverse is specified by the attribute

	   :param related: sequence of field names

	   Some field attributes are automatically copied from the source field if
	   they are not redefined: ``string``, ``help``, ``readonly``, ``required`` (only
	   if all fields in the sequence are required), ``groups``, ``digits``, ``size``,
	   ``translate``, ``sanitize``, ``selection``, ``comodel_name``, ``domain``,
	   ``context``. All semantic-free attributes are copied from the source
	   field.

	   By default, the values of related fields are not stored to the database.
	   Add the attribute ``store=True`` to make it stored, just like computed
	   fields. Related fields are automatically recomputed when their
	   dependencies are modified.

	   .. _field-company-dependent:

	   .. rubric:: Company-dependent fields

	   Formerly known as 'property' fields, the value of those fields depends
	   on the company. In other words, users that belong to different companies
	   may see different values for the field on a given record.

	   :param company_dependent: whether the field is company-dependent (boolean)

	   .. _field-incremental-definition:

	   .. rubric:: Incremental definition

	   A field is defined as class attribute on a model class. If the model
	   is extended (see :class:`~openerp.models.Model`), one can also extend
	   the field definition by redefining a field with the same name and same
	   type on the subclass. In that case, the attributes of the field are
	   taken from the parent class and overridden by the ones given in
	   subclasses.

	   For instance, the second class below only adds a tooltip on the field
	   ``state``::

	       class First(models.Model):
	           _name = 'foo'
	           state = fields.Selection([...], required=True)

	       class Second(models.Model):
	           _inherit = 'foo'
	           state = fields.Selection(help="Blah blah blah")

	*/
	TField struct {
		_type             string              // 数据存储类
		_symbol_c         string              // Format 符号 "%s,%d..."
		_symbol_f         func(string) string // Format 自定义函数
		_auto_join        bool
		_inherit          bool // 是否继承该字段指向的Model的多有字段
		foreign_field     bool // 该字段是关联表的字段
		related           bool
		model_name        string // # name of the model of this field
		comodel_name      string // 字段关联的Model # name of the model of values (if relational)
		relmodel_name     string // 字段关联的Model和字段的many2many关系表Model
		cokey_field_name  string
		relkey_field_name string
		primary_key       bool
		auto_increment    bool
		index             bool // # whether the field is indexed in database
		search            bool
		read              bool //???
		write             bool //???
		translate         bool //???
		// published exportable
		Name              string // # name of the field
		Store             bool
		Manual            bool
		Depends           []string
		Readonly          bool // 只读
		Required          bool // 字段不为空
		Help              string
		String            string
		Size              int64 // 长度大小
		Sortable          bool  // 可排序
		Searchable        bool
		Type              string                 // view 字段类型
		Default           interface{}            //# default(recs) returns the default value
		Related           string                 //???
		Relation          string                 // #关系表
		States            map[string]interface{} // #传递 UI 属性
		Selection         map[string]interface{}
		Company_dependent bool // ???
		Change_default    bool // ???

		// private membership
		Groups     string //???
		deprecated string //???
		ondelete   string //???

		_fnct_inv interface{} // ??? 函数,handler
	}

	TRelateField struct {
		// Mapping from inherits'd field name to triple (m, r, f, n) where
		// m is the model from which it is inherits'd,
		// r is the (local) field towards m,
		// f is the _column object itself,
		// n is the original (i.e. top-most) parent model.
		// Example:
		//  { 'field_name': ('parent_model', 'm2o_field_to_reach_parent',
		//                   field_column_obj, origina_parent_model), ... }
		name string
		//ParentModel         string // 继承至哪个
		//ParentM2OField      string // 外键 m2o_field_to_reach_parent
		//FieldColumn         *TField
		//OriginalParentModel *TModel // 最底层的Model
		RelateTableName   string  //idx:0
		RelateFieldName   string  //idx:1
		RelateField       *TField //idx:2
		RelateTopestTable string  //idx:3 //关联字段由那个表产生
	}
)

func NewField() *TField {
	return &TField{
		_type:     "unknown",
		_symbol_c: "%s",
		_symbol_f: _FieldFormat,
		//_deprecated: false,
		read:  true,
		write: true,

		Type: "unknown",
	}
}
func NewRelateField(aNames string, relate_table_name string, relate_field_name string, aField *TField, relate_topest_table string) *TRelateField {
	return &TRelateField{
		name:              aNames,
		RelateTableName:   relate_table_name,
		RelateFieldName:   relate_field_name,
		RelateField:       aField,
		RelateTopestTable: relate_topest_table,
	}
}

func _FieldFormat(str string) string {
	return str
}

func _CharFormat(str string) string {
	return str //`'` + str + `'`
}

// 字段关联的表
func (self *TField) RelateModelName() string {
	return self.comodel_name
}

// 多对多关系中 记录2表记录关联关系的表
func (self *TField) MiddleModelName() string {
	return self.relmodel_name
}

func (self *TField) _Type() string {
	return self._type
}

func (self *TField) SymbolChar() string {
	return self._symbol_c
}

func (self *TField) SymbolFunc() func(string) string {
	return self._symbol_f
}

func (self *TField) Translatable() bool {
	return self.translate
}

func (self *TField) Readable() bool {
	return self.read
}
func (self *TField) IsIndex() bool {
	return self.index
}

func (self *TField) IsRelated() bool {
	return self.related
}
func (self *TField) Fnct_inv() interface{} {
	return self._fnct_inv
}

/*
func (self *TField) Searchable() bool {
	return self.search
}*/
func (self *TField) Writeable() bool {
	return self.write
}
func (self *TField) IsForeignField(arg ...bool) bool {
	if len(arg) > 0 {
		self.foreign_field = arg[0]
	}
	return self.foreign_field
}
func (self *TField) IsAutoJoin() bool {
	return self._auto_join
}

// 复制一个新的一样的
func (self *TField) New() (res *TField) {
	*res = *self
	return
}

func (self *TField) _initOne2Many(arg ...string) { //comodel_name string, inverse_name string
	if len(arg) > 1 {
		self.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0])) //目标表
		self.cokey_field_name = utils.SnakeCasedName(arg[1])                 //目标表关键字段

		self.Relation = self.comodel_name
		self._type = "one2many" //TODO 剔除掉
		self.Type = "one2many"
	} else {
		logger.Panic("One2Many field ", self.Name, "'s Args can no be blank!")
	}
}

func (self *TField) _initMany2One(arg ...string) { //comodel_name string
	if len(arg) > 0 {
		self.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0])) //目标表
		self.Relation = self.comodel_name
		self._type = "many2one" //TODO 剔除掉
		self.Type = "many2one"
	} else {
		logger.Panic("Many2One field ", self.Name, "'s Args can no be blank!")
	}

}

func (self *TField) _initMany2Many(arg ...string) { //comodel_name, relation, key_field1, key_field2 string
	if len(arg) > 3 {
		self.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0]))  //目标表
		self.relmodel_name = utils.DotCasedName(utils.TitleCasedName(arg[1])) //提供目标表格关系的表
		self.cokey_field_name = utils.SnakeCasedName(arg[2])                  //目标表关键字段
		self.relkey_field_name = utils.SnakeCasedName(arg[3])                 // 关系表关键字段
		self.Relation = self.comodel_name
		self._type = "many2many" //TODO 剔除掉
		self.Type = "many2many"
	} else {
		logger.Panic("Many2Many field ", self.Name, "'s Args can no be blank!")
	}

}

// @@@
func (self *TField) _initSelection(arg ...string) { //comodel_name, relation, key_field1, key_field2 string
	if len(arg) > 3 {

		self.comodel_name = utils.DotCasedName(utils.TitleCasedName(arg[0]))  //目标表
		self.relmodel_name = utils.DotCasedName(utils.TitleCasedName(arg[1])) //提供目标表格关系的表
		self.cokey_field_name = utils.SnakeCasedName(arg[2])                  //目标表关键字段
		self.relkey_field_name = utils.SnakeCasedName(arg[3])                 // 关系表关键字段
		self.Relation = self.comodel_name
		self._type = "many2many" //TODO 剔除掉
		self.Type = "many2many"
	} else {
		logger.Panic("Many2Many field ", self.Name, "'s Args can no be blank!")
	}

}
