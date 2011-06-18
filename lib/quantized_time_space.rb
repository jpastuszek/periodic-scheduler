class RealTimeToQuantizedSpaceProjection
  def initialize(quantum_size, quantization_rule)
    @quantum_size = quantum_size
    @quantization_rule = quantization_rule
  end

  def project(value)
    @quantization_rule.call(value / @quantum_size)
  end

  def revers_project(value)
    value * @quantum_size
  end

  def projection_error(value)
    new_value = project(value)
    value - revers_project(new_value)
  end
end

