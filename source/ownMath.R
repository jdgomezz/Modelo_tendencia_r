Mode <- function(num) {
  unique_num <- unique(num)
  unique_num [which.max(tabulate(match(num, unique_num )))]
}