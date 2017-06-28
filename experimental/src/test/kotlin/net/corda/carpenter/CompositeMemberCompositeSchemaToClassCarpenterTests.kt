package net.corda.carpenter

import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.amqp.*

import org.junit.Test
import kotlin.test.assertEquals
import net.corda.carpenter.test.*

@CordaSerializable
interface I_ {
    val a: Int
}

/*
 * Where a class has a member that is also a composite type or interface
 */
class CompositeMembers : AmqpCarpenterBase() {

    @Test
    fun bothKnown () {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(val a: Int)

        @CordaSerializable
        data class B (val a: A, var b: Int)

        val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))

        assert(obj.first is B)

        val amqpObj = obj.first as B

        assertEquals(testB, amqpObj.b)
        assertEquals(testA, amqpObj.a.a)
        assertEquals(2, obj.second.schema.types.size)
        assert(obj.second.schema.types[0] is CompositeType)
        assert(obj.second.schema.types[1] is CompositeType)

        var amqpSchemaA : CompositeType? = null
        var amqpSchemaB : CompositeType? = null

        for (type in obj.second.schema.types) {
            when (type.name.split ("$").last()) {
                "A" -> amqpSchemaA = type as CompositeType
                "B" -> amqpSchemaB = type as CompositeType
            }
        }

        assert (amqpSchemaA != null)
        assert (amqpSchemaB != null)

        assertEquals(1,     amqpSchemaA?.fields?.size)
        assertEquals("a",   amqpSchemaA!!.fields[0].name)
        assertEquals("int", amqpSchemaA.fields[0].type)

        assertEquals(2,     amqpSchemaB?.fields?.size)
        assertEquals("a",   amqpSchemaB!!.fields[0].name)
//        assertEquals("${this.javaClass.name}\$${this.javaClass.enclosingMethod.name}\$A", amqpSchemaB!!.fields[0].type)
        assertEquals("b",   amqpSchemaB.fields[1].name)
        assertEquals("int", amqpSchemaB.fields[1].type)

//        var ccA = ClassCarpenter().build(amqpSchemaA.carpenterSchema())
//        var ccB = ClassCarpenter().build(amqpSchemaB.carpenterSchema())

        /*
         * Since A is known to the JVM we can't constuct B with and instance of the carpented A but
         * need to use the defined one above
         */
//        val instanceA = ccA.constructors[0].newInstance(testA)
//        val instanceB = ccB.constructors[0].newInstance(A (testA), testB)

//        assertEquals (ccA.getMethod("getA").invoke(instanceA), amqpObj.a.a)
//        assertEquals ((ccB.getMethod("getA").invoke(instanceB) as A).a, amqpObj.a.a)
//        assertEquals (ccB.getMethod("getB").invoke(instanceB), amqpObj.b)

    }

    /* you cannot have an element of a composite class we know about
       that is unknown as that should be impossible. If we have the containing
       class in the class path then we must have all of it's constituent elements */
    @Test(expected = UncarpentableException::class)
    fun nestedIsUnknown () {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(override val a: Int) : I_

        @CordaSerializable
        data class B(val a: A, var b: Int)
          val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))
        val amqpSchema = obj.second.schema.curruptName(listOf (classTestName ("A")))

        assert(obj.first is B)

        amqpSchema.carpenterSchema()
    }

    @Test
    fun ParentIsUnknown () {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(override val a: Int) : I_

        @CordaSerializable
        data class B(val a: A, var b: Int)
        val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))

        assert(obj.first is B)

        val amqpSchema = obj.second.schema.curruptName(listOf (classTestName ("B")))

        val carpenterSchema = amqpSchema.carpenterSchema()

        assertEquals (1, carpenterSchema.size)
    }

    @Test
    fun BothUnkown () {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(override val a: Int) : I_

        @CordaSerializable
        data class B(val a: A, var b: Int)
        val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))

        assert(obj.first is B)

        val amqpSchema = obj.second.schema.curruptName(listOf (classTestName ("A"), classTestName ("B")))
    }

    @Test
    fun nestedIsUnkownInherited () {
        val testA = 10
        val testB = 20
        val testC = 30

        @CordaSerializable
        open class A(val a: Int)

        @CordaSerializable
        class B(a: Int, var b: Int) : A (a)

        @CordaSerializable
        data class C(val b: B, var c: Int)

        val c = C(B(testA, testB), testC)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(c))

        assert(obj.first is C)

        val amqpSchema = obj.second.schema.curruptName(listOf (classTestName ("A"), classTestName ("B")))
    }

    @Test
    fun nestedIsUnknownInheritedUnkown () {
        val testA = 10
        val testB = 20
        val testC = 30

        @CordaSerializable
        open class A(val a: Int)

        @CordaSerializable
        class B(a: Int, var b: Int) : A (a)

        @CordaSerializable
        data class C(val b: B, var c: Int)

        val c = C(B(testA, testB), testC)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(c))

        assert(obj.first is C)

        val amqpSchema = obj.second.schema.curruptName(listOf (classTestName ("A"), classTestName ("B")))
    }

    @Test
    fun parentsIsUnknownWithUnkownInheritedMember () {
        val testA = 10
        val testB = 20
        val testC = 30

        @CordaSerializable
        open class A(val a: Int)

        @CordaSerializable
        class B(a: Int, var b: Int) : A (a)

        @CordaSerializable
        data class C(val b: B, var c: Int)

        val c = C(B(testA, testB), testC)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(c))

        assert(obj.first is C)

        val amqpSchema = obj.second.schema.curruptName(listOf (classTestName ("A"), classTestName ("B")))
    }


    /*
     * In this case B holds an element of Interface I_ which is an A but we don't know of A
     * but we do know about I_
     */
    @Test
    fun nestedIsInterfaceToUnknown () {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(override val a: Int) : I_

        @CordaSerializable
        data class B(val a: A, var b: Int)
          val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))

        assert(obj.first is B)
    }

    @Test
    fun nestedIsUnknownInterface() {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(override val a: Int) : I_

        @CordaSerializable
        data class B(val a: A, var b: Int)
        val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))

        assert(obj.first is B)
    }

    @Test
    fun ParentsIsInterfaceToUnkown() {
        val testA = 10
        val testB = 20

        @CordaSerializable
        data class A(override val a: Int) : I_

        @CordaSerializable
        data class B(val a: A, var b: Int)
        val b = B(A(testA), testB)

        val obj = DeserializationInput(factory).deserializeRtnEnvelope(serialise(b))

        assert(obj.first is B)
    }
}

