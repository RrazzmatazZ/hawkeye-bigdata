package com.dreamtail.stream.common

trait TService extends Serializable {
  /**
   * 执行分析服务
   *
   * @return
   */
  def analysis(): Any
}
