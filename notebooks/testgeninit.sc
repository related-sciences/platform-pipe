// Collectively, these genes have at least one record for each data source
lazy val GENE_SET_1 = List(
  "ENSG00000141510", // TP53
  "ENSG00000169174", // PCSK9
  "ENSG00000105397"  // TYK2
)

// Dumps of uniprot/ensembl/nonreference ids in both evidence and gene index:
// +----------+-------------------------+---------------+
// |uniprot_id|       uniprot_accessions|ensembl_gene_id|
// +----------+-------------------------+---------------+
// |    P35354| [Q16876, A8K802, P35354]|ENSG00000073756|
// |    P10275|[B1AKD7, A2RUN2, E7EVX...|ENSG00000169083|
// |    Q01668|[Q9UDC3, Q01668, Q71UT...|ENSG00000157388|
// |    Q13936|[Q14743, Q99025, Q1392...|ENSG00000151067|
// |    Q13698|[A4IF51, Q12896, Q1369...|ENSG00000081248|
// |    P62942|[P62942, Q4VC47, Q9H10...|ENSG00000088832|
// |    P08588|[Q5T5Y4, P08588, Q9UKG...|ENSG00000043591|
// |    P00915|                 [P00915]|ENSG00000133742|
// |    P23219|[B4E2S5, Q3HY28, Q1512...|ENSG00000095303|
// |    O60840|[Q9UHB1, O43901, F5CIQ...|ENSG00000102001|
// +----------+-------------------------+---------------+
// +---------------+-----------+---------------+---------------+
// |            tid|gene_symbol|      reference|      alternate|
// +---------------+-----------+---------------+---------------+
// |ENSG00000183214|       MICA|ENSG00000204520|ENSG00000183214|
// |ENSG00000223532|      HLA-B|ENSG00000234745|ENSG00000223532|
// |ENSG00000227715|      HLA-A|ENSG00000206503|ENSG00000227715|
// |ENSG00000231179|       MICB|ENSG00000204516|ENSG00000231179|
// |ENSG00000235233|       MICA|ENSG00000204520|ENSG00000235233|
// |ENSG00000206240|   HLA-DRB1|ENSG00000196126|ENSG00000206240|
// |ENSG00000231823|   HLA-DQA2|ENSG00000237541|ENSG00000231823|
// |ENSG00000225845|      BTNL2|ENSG00000204290|ENSG00000225845|
// |ENSG00000276146|     LILRB2|ENSG00000131042|ENSG00000276146|
// |ENSG00000277594|      DDX52|ENSG00000278053|ENSG00000277594|
// +---------------+-----------+---------------+---------------+

// These genes cover non-Ensembl source records
lazy val GENE_MAP_1 = Map(
  // Non-reference genes
  "ENSG00000223532" -> "ENSG00000234745", // HLA-B
  "ENSG00000225845" -> "ENSG00000204290", // BTNL2
  // UniProt entries
  "P35354" -> "ENSG00000073756",
  "P10275" -> "ENSG00000169083"
)
