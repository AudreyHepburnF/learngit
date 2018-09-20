##采购项目
  ##查询总数
SELECT
	COUNT(*)
FROM
	bmpfjz_supplier_project_bid bspb
LEFT JOIN bmpfjz_project bp ON bspb.project_id = bp.id
LEFT JOIN bmpfjz_project_ext bpe ON bspb.project_id = bpe.id
WHERE
	bspb.supplier_bid_status = 6
AND bpe.publish_bid_result_time > ?;

  ##查询信息
SELECT
	bp.id projectId,
	bp.`name` projectName,
	bp.`name` projectNameNotAnalyzed,
	bp.`code` projectCode,
	bp.comp_id companyId,
	bp.comp_name companyName,
	bp.comp_name companyNameNotAnalyzed,
	bspb.supplier_id supplierId,
	bspb.supplier_name supplierNameNotAnalyzed,
	bspb.deal_total_price dealTotalPrice,
	bpe.publish_bid_result_time dealTime
FROM
	bmpfjz_supplier_project_bid bspb
LEFT JOIN bmpfjz_project bp ON bspb.project_id = bp.id
LEFT JOIN bmpfjz_project_ext bpe ON bspb.project_id = bpe.id
WHERE
	bspb.supplier_bid_status = 6
AND bpe.publish_bid_result_time > ?
limit ?,?;

##招标项目
	##查询总数
SELECT
	COUNT(*)
FROM
	bid b
	LEFT JOIN proj_inter_project pip ON b.PROJECT_ID = pip.ID
	LEFT JOIN bid_decided bd ON b.PROJECT_ID = bd.PROJECT_ID
WHERE
	b.IS_BID_SUCCESS = 1
	AND bd.UPDATE_DATE > ?
	AND pip.PROJECT_STATUS IN (9, 11)
	AND NOT EXISTS (
			SELECT
				1
			FROM
				bid_decided bd1
			WHERE
				bd.ROUND < bd1.ROUND
				AND bd.PROJECT_ID = bd1.PROJECT_ID
	);


	##查询信息
SELECT
	pip.id projectId,
	pip.PROJECT_NAME projectName,
	pip.PROJECT_NAME projectNameNotAnalyzed,
	pip.PROJECT_NUMBER projectCode,
	pip.COMPANY_ID companyId,
	b.BIDER_ID supplierId,
	b.BIDER_NAME supplierNameNotAnalyzed,
	b.BIDER_PRICE_UNE dealTotalPrice,
	bd.UPDATE_DATE dealTime
FROM
	bid b
	LEFT JOIN proj_inter_project pip ON b.PROJECT_ID = pip.ID
	LEFT JOIN bid_decided bd ON b.PROJECT_ID = bd.PROJECT_ID
WHERE
	b.IS_BID_SUCCESS = 1
	AND bd.UPDATE_DATE > ?
	AND pip.PROJECT_STATUS IN (9, 11)
	AND NOT EXISTS (
			SELECT
				1
			FROM
				bid_decided bd1
			WHERE
				bd.ROUND < bd1.ROUND
				AND bd.PROJECT_ID = bd1.PROJECT_ID
	) LIMIT ?,?;

##竞价项目
	##查询打包的项目数量
SELECT
	COUNT(*)
FROM
	auction_bid_supplier abs
	LEFT JOIN auction_project ap ON abs.project_id = ap.id
WHERE
	abs.bid_status = 1
AND ap.publish_result_time > ?
AND ap.project_type = 1;

	##查询打包的项目信息
SELECT
	ap.id projectId,
	ap.project_name projectName,
	ap.project_name projectNameNotAnalyzed,
	ap.project_code projectCode,
	ap.comp_id companyId,
	abs.supplier_id supplierId,
	abs.supplier_name supplierNameNotAnalyzed,
	ap.publish_result_time dealTime,
	IFNULL(abs.final_price,abs.real_price) dealTotalPrice
FROM
	auction_bid_supplier abs
LEFT JOIN auction_project ap ON abs.project_id = ap.id
WHERE
	abs.bid_status = 1
AND ap.publish_result_time > ?
AND ap.project_type = 1
LIMIT ?,?;

	##查询非打包的项目数量
SELECT
	COUNT(*)
FROM
	(
		SELECT
			ap.id projectId,
			abs.supplier_id supplierId
		FROM
			auction_bid_supplier abs
			LEFT JOIN auction_project ap ON abs.project_id = ap.id
			LEFT JOIN auction_directory_info adi ON (abs.directory_id=adi.directory_id AND abs.project_id=adi.project_id)
		WHERE
			abs.bid_status = 1
			AND ap.publish_result_time > 0
			AND ap.project_type = 2
			AND abs.divide_rate IS NOT NULL
		GROUP BY
			abs.project_id,
			abs.supplier_id
	) p;



####
SELECT
	ap.id projectId,
	ap.project_name projectName,
	ap.project_name projectNameNotAnalyzed,
	ap.project_code projectCode,
	ap.comp_id companyId,
	abs.supplier_id supplierId,
	abs.supplier_name supplierNameNotAnalyzed,
	ap.publish_result_time dealTime,
	SUM(IFNULL(abs.final_price,abs.real_price)*adi.plan_amount*abs.divide_rate/100) dealTotalPrice
FROM
	auction_bid_supplier abs
LEFT JOIN auction_project ap ON abs.project_id = ap.id
LEFT JOIN auction_directory_info adi ON (abs.directory_id=adi.directory_id AND abs.project_id=adi.project_id)
WHERE
	abs.bid_status = 1
AND ap.publish_result_time > ?
AND ap.project_type = 2
AND abs.divide_rate IS NOT NULL
GROUP BY
	abs.project_id,
	abs.supplier_id
LIMIT ?,?;