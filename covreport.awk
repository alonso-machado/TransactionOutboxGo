#!/usr/bin/awk -f
# Statement-weighted coverage per package from a Go coverage profile.
# Profile line format: <path>:<start>.<col>,<end>.<col> <numStmts> <count>
NR == 1 { next }  # skip "mode:" line
{
  loc = $1
  colon = index(loc, ":")
  path = substr(loc, 1, colon - 1)            # file path
  # package = dirname(path)
  slash = path
  while (length(slash) > 0 && substr(slash, length(slash), 1) != "/") {
    slash = substr(slash, 1, length(slash) - 1)
  }
  pkg = substr(slash, 1, length(slash) - 1)
  stmts = $2 + 0
  cnt = $3 + 0
  total[pkg] += stmts
  if (cnt > 0) covered[pkg] += stmts
}
END {
  for (p in total) {
    pct = (total[p] > 0) ? 100.0 * covered[p] / total[p] : 0
    printf "%6.1f%%  %s\n", pct, p
  }
}
