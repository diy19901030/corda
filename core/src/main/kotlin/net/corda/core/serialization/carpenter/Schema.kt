package net.corda.core.serialization.carpenter

import org.objectweb.asm.Type
import java.util.LinkedHashMap

class ClassSchema(
    name: String,
    fields: Map<String, Class<out Any?>>,
    superclass: Schema? = null,
    interfaces: List<Class<*>> = emptyList()
) : ClassCarpenter.Schema (name, fields, superclass, interfaces)

class InterfaceSchema(
    name: String,
    fields: Map<String, Class<out Any?>>,
    superclass: Schema? = null,
    interfaces: List<Class<*>> = emptyList()
) : ClassCarpenter.Schema (name, fields, superclass, interfaces)
