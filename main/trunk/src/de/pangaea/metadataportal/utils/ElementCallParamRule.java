package de.pangaea.metadataportal.utils;

import org.apache.commons.digester.Rule;
import org.xml.sax.Attributes;

/**
 * <p>
 * TODO
 * </p>
 */
public final class ElementCallParamRule extends Rule {
  
  /**
   * Construct a "call parameter" rule that will save the body text of this
   * element as the parameter value.
   *
   * @param paramIndex
   *          The zero-relative parameter number
   */
  public ElementCallParamRule(int paramIndex) {
    this.paramIndex = paramIndex;
  }
  
  @Override
  public void begin(String namespace, String name, Attributes attributes)
      throws Exception {
    
    String param = getDigester().getCurrentElementName();
    
    if (param != null) {
      Object parameters[] = (Object[]) digester.peekParams();
      parameters[paramIndex] = param;
    }
    
  }

  private final int paramIndex;
}
