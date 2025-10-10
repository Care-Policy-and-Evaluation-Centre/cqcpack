# install.packages("../cqcpack_0.1.2.tar.gz", repos = NULL, type = "source")


local({
    withr::local_options(list("cqc.data_stale_after_days" = 0L))
    testthat::expect_warning(library(cqcpack))
})
