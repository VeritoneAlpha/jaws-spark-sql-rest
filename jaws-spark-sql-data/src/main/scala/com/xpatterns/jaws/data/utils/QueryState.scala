package com.xpatterns.jaws.data.utils

object QueryState extends Enumeration {
  type QueryState = Value
  val DONE, IN_PROGRESS, FAILED, NOT_FOUND = Value
}