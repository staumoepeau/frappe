import frappe
from frappe.model.naming import (
	BRACED_PARAMS_WORD_PATTERN,
	NAMING_SERIES_PART_TYPES,
	determine_consecutive_week_number,
	has_custom_parser,
)
from frappe.query_builder import DocType
from frappe.utils import cstr

def execute():
	Series = DocType("Series")
	# Get all DocTypes with naming_rule = "expression"
	doctypes = frappe.get_all("DocType", filters={"naming_rule": "expression"}, fields=["name", "autoname"], limit_page_length=100)
	uniq_exprs = set()

	def get_param_value_for_word_match(doc):
		def get_param_value(match):
			e = match.group()[1:-1]
			creation = doc.creation if hasattr(doc, "creation") else frappe.utils.now_datetime()
			part = ""

			if e == "YY":
				part = creation.strftime("%y")
			elif e == "MM":
				part = creation.strftime("%m")
			elif e == "DD":
				part = creation.strftime("%d")
			elif e == "YYYY":
				part = creation.strftime("%Y")
			elif e == "WW":
				part = determine_consecutive_week_number(creation)
			elif e == "timestamp":
				part = str(creation)
			elif doc and doc.get(e):
				part = doc.get(e)
			elif method := has_custom_parser(e):
				part = frappe.get_attr(method[0])(doc, e)
			else:
				part = e

			return cstr(part).strip()

		return get_param_value

	for doctype in doctypes:
		if "#" in doctype.autoname:
			# Limit document retrieval to 50 per batch
			docs = frappe.get_all(doctype.name, fields=["name"], limit_page_length=50)
			for doc in docs:
				_doc = frappe.get_doc(doctype.name, doc.name)
				expr = doctype.autoname[7 : doctype.autoname.find("{#")]
				key = BRACED_PARAMS_WORD_PATTERN.sub(get_param_value_for_word_match(_doc), expr)
				uniq_exprs.add(key)

	# Fetch current series counter
	current_series = frappe.qb.from_(Series).select(Series.name, Series.current).run(as_dict=True)
	existing_series = {row["name"]: row["current"] for row in current_series}

	for uniq_expr in uniq_exprs:
		if uniq_expr not in existing_series:
			frappe.db.insert({"doctype": "Series", "name": uniq_expr, "current": 1})

	frappe.db.commit()
