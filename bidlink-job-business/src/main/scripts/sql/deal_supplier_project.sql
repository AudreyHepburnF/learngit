##采购项目
  ##查询总数
SELECT
	COUNT(*)
FROM
	purchase_supplier_project psp
LEFT JOIN purchase_project pp ON psp.project_id = pp.id
LEFT JOIN purchase_project_ext ppe ON psp.project_id = ppe.id
WHERE
	psp.deal_status = 2
AND pp.id IS NOT NULL
AND ppe.publish_result_time > ?;

  ##查询信息
SELECT
	pp.id projectId,
	pp.`name` projectName,
	pp.`name` projectNameNotAnalyzed,
	pp.`code` projectCode,
	pp.company_id companyId,
	pp.company_name companyName,
	pp.company_name companyNameNotAnalyzed,
	psp.supplier_id supplierId,
	psp.supplier_name supplierNameNotAnalyzed,
	ppe.publish_result_time dealTime,
	psp.deal_total_price dealTotalPrice
FROM
	purchase_supplier_project psp
LEFT JOIN purchase_project pp ON psp.project_id = pp.id
LEFT JOIN purchase_project_ext ppe ON psp.project_id = ppe.id
WHERE
	psp.deal_status = 2
AND pp.id IS NOT NULL
AND ppe.publish_result_time > ?
LIMIT ?,?;

##招标项目
	##查询总数
SELECT
	COUNT(*)
FROM
	bid_supplier bs
WHERE
	bs.win_bid_status = 1
AND bs.win_bid_time > ?;

	##查询信息
SELECT
	bs.sub_project_id projectId,
	bs.project_name projectName,
	bs.project_name projectNameNotAnalyzed,
	bs.project_code projectCode,
	bs.company_id companyId,
	bs.company_name companyName,
	bs.company_name companyNameNotAnalyzed,
	bs.supplier_id supplierId,
	bs.supplier_name supplierNameNotAnalyzed,
	bs.win_bid_time dealTime,
	bs.win_bid_total_price dealTotalPrice
FROM
	bid_supplier bs
WHERE
	bs.win_bid_status = 1
AND bs.win_bid_time > ?
LIMIT ?,?;

##竞价项目
	##查询总数
SELECT
	COUNT(*)
FROM
	auction_supplier_project asp
	LEFT JOIN auction_project ap ON asp.project_id = ap.id
	LEFT JOIN auction_project_ext ape ON asp.project_id = ape.id
WHERE
	asp.deal_status = 3
AND ap.id IS NOT NULL
AND ape.publish_result_time > ?;

	##查询信息
SELECT
	ap.id projectId,
	ap.`name` projectName,
	ap.`name` projectNameNotAnalyzed,
	ap.`code` projectCode,
	ap.company_id companyId,
	ap.company_name companyName,
	ap.company_name companyNameNotAnalyzed,
	asp.supplier_id supplierId,
	asp.supplier_name supplierNameNotAnalyzed,
	ape.publish_result_time dealTime,
	asp.deal_total_price dealTotalPrice
FROM
	auction_supplier_project asp
	LEFT JOIN auction_project ap ON asp.project_id = ap.id
	LEFT JOIN auction_project_ext ape ON asp.project_id = ape.id
WHERE
	asp.deal_status = 3
AND ap.id IS NOT NULL
AND ape.publish_result_time > ?
LIMIT ?,?;