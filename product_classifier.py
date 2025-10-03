"""
Product Classification Module
Maps product names/descriptions to standardized product types for search optimization
"""

class ProductClassifier:
    """
    Handles product type classification using keyword matching and AI fallbacks
    """
    
    # Comprehensive product type mappings
    PRODUCT_MAPPINGS = {
        'phone': [
            'phone', 'iphone', 'pixel', 'samsung', 'galaxy', 'smartphone',
            'mobile', 'android', 'ios', 'huawei', 'oneplus', 'xiaomi',
            'oppo', 'vivo', 'realme', 'motorola', 'nokia'
        ],
        'laptop': [
            'laptop', 'macbook', 'computer', 'notebook', 'chromebook',
            'thinkpad', 'dell', 'hp', 'lenovo', 'asus', 'acer', 'surface'
        ],
        'tablet': [
            'tablet', 'ipad', 'kindle', 'surface pro', 'galaxy tab'
        ],
        'tv': [
            'tv', 'television', 'smart tv', 'oled', 'qled', 'monitor',
            '4k', '8k', 'hdtv', 'screen', 'display'
        ],
        'headphones': [
            'headphone', 'earphone', 'earbud', 'airpod', 'beats',
            'headset', 'earpiece', 'audio'
        ],
        'watch': [
            'watch', 'smartwatch', 'apple watch', 'fitbit', 'garmin',
            'timepiece', 'wristwatch'
        ],
        'camera': [
            'camera', 'dslr', 'mirrorless', 'gopro', 'canon', 'nikon',
            'sony camera', 'webcam'
        ],
        'speaker': [
            'speaker', 'bluetooth speaker', 'soundbar', 'jbl', 'bose',
            'echo', 'home pod'
        ],
        'shoes': [
            'shoe', 'sneaker', 'boot', 'sandal', 'slipper', 'heel',
            'trainer', 'footwear', 'nike', 'adidas', 'puma', 'loafer',
            'flip flop', 'croc'
        ],
        'shirt': [
            'shirt', 't-shirt', 'tshirt', 'tee', 'blouse', 'polo',
            'tank top', 'top'
        ],
        'pants': [
            'pant', 'jean', 'trouser', 'chino', 'slack', 'legging',
            'jogger', 'sweatpant'
        ],
        'dress': [
            'dress', 'gown', 'frock', 'sundress', 'maxi dress'
        ],
        'jacket': [
            'jacket', 'coat', 'blazer', 'hoodie', 'cardigan',
            'windbreaker', 'parka', 'bomber'
        ],
        'sweater': [
            'sweater', 'jumper', 'pullover', 'sweatshirt', 'knit'
        ],
        'bag': [
            'bag', 'backpack', 'purse', 'handbag', 'satchel', 'tote',
            'clutch', 'duffel', 'messenger bag', 'wallet'
        ],
        'jewelry': [
            'jewelry', 'necklace', 'bracelet', 'ring', 'earring',
            'pendant', 'chain', 'anklet', 'brooch'
        ],
        'sunglasses': [
            'sunglass', 'shades', 'eyewear', 'spectacle', 'glass'
        ],
        'perfume': [
            'perfume', 'cologne', 'fragrance', 'scent', 'eau de'
        ],
        'makeup': [
            'makeup', 'cosmetic', 'foundation', 'lipstick', 'mascara',
            'eyeshadow', 'blush', 'concealer'
        ],
        'skincare': [
            'cream', 'lotion', 'serum', 'moisturizer', 'cleanser',
            'toner', 'sunscreen', 'skincare', 'face wash'
        ],
        'soap': [
            'soap', 'body wash', 'shower gel', 'hand wash', 'wash',
            'gel', 'shampoo', 'conditioner'
        ],
        'flowers': [
            'flower', 'rose', 'bouquet', 'stem', 'lily', 'tulip',
            'orchid', 'daisy', 'carnation', 'sunflower', 'plant'
        ],
        'cake': [
            'cake', 'cupcake', 'pastry', 'dessert', 'torte',
            'sponge cake', 'cheesecake'
        ],
        'bread': [
            'bread', 'bun', 'roll', 'loaf', 'baguette', 'croissant'
        ],
        'coffee': [
            'coffee', 'espresso', 'latte', 'cappuccino', 'mocha',
            'americano', 'bean'
        ],
        'tea': [
            'tea', 'green tea', 'black tea', 'herbal tea', 'chai'
        ],
        'juice': [
            'juice', 'smoothie', 'beverage', 'drink', 'nectar'
        ],
        'wine': [
            'wine', 'champagne', 'prosecco', 'red wine', 'white wine'
        ],
        'beer': [
            'beer', 'ale', 'lager', 'stout', 'ipa'
        ],
        'book': [
            'book', 'novel', 'textbook', 'ebook', 'paperback',
            'hardcover', 'magazine', 'journal'
        ],
        'toy': [
            'toy', 'action figure', 'doll', 'puzzle', 'lego',
            'game', 'playset'
        ],
        'furniture': [
            'chair', 'table', 'sofa', 'couch', 'bed', 'desk',
            'cabinet', 'shelf', 'drawer', 'stool', 'bench'
        ],
        'mattress': [
            'mattress', 'bed frame', 'pillow', 'bedding'
        ],
        'lamp': [
            'lamp', 'light', 'bulb', 'chandelier', 'lantern'
        ],
        'cookware': [
            'pot', 'pan', 'skillet', 'wok', 'cookware', 'utensil'
        ],
        'blender': [
            'blender', 'mixer', 'juicer', 'grinder', 'processor'
        ],
        'microwave': [
            'microwave', 'oven', 'toaster', 'air fryer'
        ],
        'refrigerator': [
            'refrigerator', 'fridge', 'freezer', 'cooler'
        ],
        'vacuum': [
            'vacuum', 'cleaner', 'hoover', 'sweeper'
        ],
        'fan': [
            'fan', 'air conditioner', 'ac', 'cooler', 'heater'
        ],
        'bicycle': [
            'bicycle', 'bike', 'cycle', 'mountain bike', 'road bike'
        ],
        'car': [
            'car', 'vehicle', 'automobile', 'sedan', 'suv', 'truck'
        ],
        'tire': [
            'tire', 'tyre', 'wheel', 'rim'
        ],
        'battery': [
            'battery', 'charger', 'power bank', 'adapter'
        ],
        'guitar': [
            'guitar', 'bass', 'ukulele', 'string instrument'
        ],
        'keyboard': [
            'keyboard', 'piano', 'synthesizer', 'keys'
        ],
        'drum': [
            'drum', 'percussion', 'cymbal', 'snare'
        ],
        'vitamins': [
            'vitamin', 'supplement', 'mineral', 'tablet', 'capsule',
            'pill', 'medicine'
        ],
        'protein': [
            'protein', 'whey', 'supplement', 'powder', 'shake'
        ],
        'diapers': [
            'diaper', 'nappy', 'wipe', 'baby care'
        ],
        'stroller': [
            'stroller', 'pram', 'pushchair', 'baby carrier'
        ],
        'paint': [
            'paint', 'brush', 'roller', 'coating'
        ],
        'tool': [
            'tool', 'hammer', 'screwdriver', 'drill', 'wrench',
            'saw', 'plier'
        ],
        'seeds': [
            'seed', 'seedling', 'bulb', 'fertilizer'
        ],
        'pet food': [
            'dog food', 'cat food', 'pet food', 'kibble', 'treat'
        ],
        'collar': [
            'collar', 'leash', 'harness', 'pet accessory'
        ]
    }
    
    # Category words to avoid (too broad)
    INVALID_CATEGORIES = [
        'retail', 'wholesale', 'telecommunications', 'electronics',
        'food', 'beverage', 'clothing', 'footwear', 'furniture',
        'beauty', 'personal', 'care', 'real', 'estate', 'fashion',
        'apparel', 'accessories', 'home', 'garden', 'sports',
        'automotive', 'health', 'toys', 'books', 'music'
    ]
    
    @classmethod
    def classify_from_text(cls, name, description=""):
        """
        Classify a product based on its name and description using keyword matching
        
        Args:
            name (str): Product name
            description (str): Product description
            
        Returns:
            str: Product type or None if no match found
        """
        # Combine name and description for searching
        search_text = f"{name} {description}".lower()
        
        # Check each product type's keywords
        for product_type, keywords in cls.PRODUCT_MAPPINGS.items():
            for keyword in keywords:
                if keyword in search_text:
                    return product_type
        
        return None
    
    @classmethod
    def is_valid_product_type(cls, product_type):
        """
        Check if a product type is valid (not a broad category)
        
        Args:
            product_type (str): Product type to validate
            
        Returns:
            bool: True if valid, False if it's a broad category
        """
        return product_type.lower() not in cls.INVALID_CATEGORIES
    
    @classmethod
    def get_ai_examples(cls):
        """
        Returns a list of example conversations for AI few-shot learning
        
        Returns:
            list: List of dictionaries with role/content for OpenAI API
        """
        return [
            {
                "role": "user", 
                "content": "Product name: Pixel 9\nDescription: smartphone with camera\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "phone"
            },
            {
                "role": "user", 
                "content": "Product name: iPhone 16\nDescription: Apple smartphone with A18 chip\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "phone"
            },
            {
                "role": "user", 
                "content": "Product name: Nike Air Max\nDescription: athletic sneakers\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "shoes"
            },
            {
                "role": "user", 
                "content": "Product name: MacBook Pro M3\nDescription: Apple laptop computer\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "laptop"
            },
            {
                "role": "user", 
                "content": "Product name: Red roses 20 stems\nDescription: bouquet of roses\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "flowers"
            },
            {
                "role": "user", 
                "content": "Product name: Chocolate fudge cake\nDescription: dessert with layers\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "cake"
            },
            {
                "role": "user", 
                "content": "Product name: Sony WH-1000XM5\nDescription: noise cancelling headphones\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "headphones"
            },
            {
                "role": "user", 
                "content": "Product name: Naruto hoodie\nDescription: anime themed sweater\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "hoodie"
            }
        ]
    
    @classmethod
    def get_all_product_types(cls):
        """
        Returns a list of all available product types
        
        Returns:
            list: Sorted list of product type names
        """
        return sorted(cls.PRODUCT_MAPPINGS.keys())